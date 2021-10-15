package main

import (
	"context"
	"flag"
	"fmt"
	"log"
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

var (
	channel           = flag.String("channel","15", "Channel for input")
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	local           = flag.String("local","", "Local sxserv addr")
	idlist          []uint64
	spMap           map[uint64]*sxutil.SupplyOpts
	mu              sync.Mutex
	sxServerAddress string
	supplyClt       *sxutil.SXServiceClient
)

type Channel struct {
	Channel string `json:"channel"`
}

type MyFleet struct {
	VehicleId  int                    `json:"vehicle_id"`
	Status     int                    `json:"status"`
	Coord      map[string]interface{} `json:"coord"`
	Angle      float32                `json:"angle"`
	Speed      int                    `json:"speed"`
	MyServices map[string]interface{} `json"services"`
	Demands    []int                  `json:"demands"`
}

type MyVehicle struct {
	vehicles []*MyFleet `json:"vehicles"`
}

type MyJson map[string]interface{}

func init() {
	idlist = make([]uint64, 0)
	spMap = make(map[uint64]*sxutil.SupplyOpts)
}

// callback for each Supplu
func suppluCallback(clt *sxutil.SXServiceClient, sp *pb.Supply) {
	// check if demand is match with my supply.

	if sp.GetSupplyName() == "stdin" { // for usual data
		datastr := sp.GetArgJson()
		token :=  strings.Split(datastr, ",")

		if strings.HasPrefix(token[0],"NisshinEisei-OBD-") {
			carnum := token[0][17:]
			cn , _ := strconv.Atoi(carnum)
			vehicle_id := 10000+ cn
			lat, _ := strconv.ParseFloat(token[3],64)
			lon, _ := strconv.ParseFloat(token[4],64)
			speed,_ := strconv.ParseFloat(token[5],64)
			angle,_ := strconv.ParseFloat(token[6],64)
			log.Printf("CarNUm:%s, %f, %f, %f, %f", carnum, cn, lat, lon, speed, angle)

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
}

// supply for channel data
func subscribeSupply(client *sxutil.SXServiceClient) {
	// goroutine!
	ctx := context.Background() //
	client.SubscribeSupply(ctx, suppluCallback)
	// comes here if channel closed
	log.Printf("Server closed... on conv_packer provider")
}


func main() {
	log.Printf("ConvPacker(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	ch,cherr := strconv.Atoi(*channel)
	if cherr != nil{
		log.Fatal("Can't convert channel num ",*channel)
	}

	recvChan :=uint32(ch)

	channelTypes :=[]uint32{recvChan,uint32(pbase.RIDE_SHARE)}
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
	}else{
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
