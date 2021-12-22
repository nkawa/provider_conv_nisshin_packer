package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"

	fleet "github.com/synerex/proto_fleet"
	pb "github.com/synerex/synerex_api"
	nodeapi "github.com/synerex/synerex_nodeapi"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"
)

type LatLon struct {
	Lat   float64
	Lon   float64
	Angle float64
}

var (
	channel         = flag.String("channel", "15", "Channel for input")
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	local           = flag.String("local", "", "Local sxserv addr")
	posMap          map[int32]LatLon
	mu              sync.Mutex
	sxServerAddress string
	supplyClt       *sxutil.SXServiceClient
)

func init() {
	posMap = make(map[int32]LatLon)
}

// for on board devices
var carPrefix = []string{
	"NisshinEisei-OBD-",
	"HinodeEisei-OBD-",
	"Nikkan-OBD-",
	"ToyotaEisei-OBD-",
}

// for sensor devices
var sensors = []string{
	"600002",
	"600004",
	"600006",
}

// callback for each Supplu
func supplyCallback(clt *sxutil.SXServiceClient, sp *pb.Supply) {
	// check if demand is match with my supply.

	if sp.GetSupplyName() == "stdin" { // for usual data
		datastr := sp.GetArgJson()
		token := strings.Split(datastr, ",")
		var vehicle_id int32 = 0
		for _, prefix := range carPrefix {
			if strings.HasPrefix(token[0], prefix) {
				carnum := token[0][len(prefix):]
				cn, _ := strconv.Atoi(carnum)
				vehicle_id = int32(10000 + cn)
				break
			}
		}
		if vehicle_id == 0 { // other cars?
			for _, sname := range sensors {
				if token[0] == sname {
					cn, _ := strconv.Atoi(sname)
					vehicle_id = int32(cn)
					break
				}
			}
		}
		if vehicle_id == 0 {
			log.Printf("IngoreData:", datastr)
			return
		}
		lat, err := strconv.ParseFloat(token[3], 64)
		if err != nil || lat < 20.0 || lat > 46.0 {
			log.Printf("Err lat %s",token[3])
		//	log.Printf("err parse lat [%s] %s", token[3], err.Error())
			return
		}
		lon, err2 := strconv.ParseFloat(token[4], 64)
		if err2 != nil || lon < 122.0 || lon > 154.0 {
			log.Printf("Err lon %s",token[4])
//			log.Printf("err parse lon [%s] %s", token[4], err.Error())
			return
		}
		altitude, _ := strconv.ParseFloat(token[5], 64)
		speed, _ := strconv.ParseFloat(token[6], 64)

		lastPos, ok := posMap[vehicle_id]
		var angle, dist float64
		if ok { // まあだいたいでいいよね？ (北緯35 度の1度あたりの距離で計算)
			dlat := (lastPos.Lat - lat) * 110940.5844 // 110km / degree
			dlon := (lastPos.Lon - lon) * 91287.7885  // 91km / degree
			// あまりに移動が小さい場合は、考える。
			dist = math.Sqrt(dlat*dlat + dlon*dlon)
			if speed > 0.1 && dist > 2 { // need to fix
				angle = math.Atan2(dlon, dlat)*180/math.Pi + 180
			} else {
				angle = lastPos.Angle
			}
		} else {
			dist = 0
			angle = 0
		}
		lastPos.Lat = lat
		lastPos.Lon = lon
		lastPos.Angle = angle
		posMap[vehicle_id] = lastPos

		log.Printf("CarNUm:%6d, %f, %f, alt:%f spd:%f dst:%f agl %f ", vehicle_id, lat, lon, altitude, speed, dist, angle)

		if lat < 30 || lat > 40 || lon < 120 || lon > 150 {
			log.Printf("error too big!")
			return
		}

		// Make Protobuf Message from JSON
		fleet := fleet.Fleet{
			VehicleId: int32(vehicle_id),
			Angle:     float32(angle),
			Speed:     int32(speed),
			Status:    int32(0),
			Coord: &fleet.Fleet_Coord{
				Lat: float32(lat),
				Lon: float32(lon),
			},
		}
		out, err := proto.Marshal(&fleet)
		if err == nil {
			cont := pb.Content{Entity: out}
			// Register supply
			smo := sxutil.SupplyOpts{
				Name:  "Map Supply",
				Cdata: &cont,
			}
			//			fmt.Printf("Res: %v",smo)
			_, nerr := supplyClt.NotifySupply(&smo)
			if nerr != nil { // connection failuer with current client
				// we need to ask to nodeidserv?
				// or just reconnect.
				newClient := sxutil.GrpcConnectServer(sxServerAddress)
				if newClient != nil {
					log.Printf("Reconnect Server %s\n", sxServerAddress)
					supplyClt.SXClient = newClient
				}
			}

		}

	}
}

// supply for channel data
func subscribeSupply(client *sxutil.SXServiceClient) {
	// goroutine!
	ctx := context.Background() //
	client.SubscribeSupply(ctx, supplyCallback)
	// comes here if channel closed
	log.Printf("Server closed... on conv_packer provider")
}

func main() {
	log.Printf("ConvPacker(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	ch, cherr := strconv.Atoi(*channel)
	if cherr != nil {
		log.Fatal("Can't convert channel num ", *channel)
	}

	recvChan := uint32(ch)

	channelTypes := []uint32{recvChan, uint32(pbase.RIDE_SHARE)}
	sxo := &sxutil.SxServerOpt{
		NodeType:   nodeapi.NodeType_PROVIDER,
		ServerInfo: "ConvPacker",
		ClusterId:  0,
		AreaId:     "Default",
	}

	// obtain synerex server address from nodeserv
	srv, err := sxutil.RegisterNode(
		*nodesrv,
		"ConvPacker",
		channelTypes,
		sxo,
	)

	if err != nil {
		log.Fatal("Can't register node...")
	}
	log.Printf("Connecting Server [%s]\n", srv)

	wg := sync.WaitGroup{} // for syncing other goroutines
	if *local != "" {
		sxServerAddress = *local
	} else {
		sxServerAddress = srv
	}
	client := sxutil.GrpcConnectServer(sxServerAddress)
	argJson := fmt.Sprintf("{ConvPacker:recv}")
	sclient := sxutil.NewSXServiceClient(client, recvChan, argJson)

	argJson = fmt.Sprintf("{ConvPacker:send}")
	supplyClt = sxutil.NewSXServiceClient(client, pbase.RIDE_SHARE, argJson)

	wg.Add(1)
	go subscribeSupply(sclient)

	wg.Wait()
	sxutil.CallDeferFunctions() // cleanup!

}
