package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	nsq "github.com/nsqio/go-nsq"
)

func main() {
	client := NewProducerClient([]string{"127.0.0.1:4161", "127.0.0.1:4261"})
        time.Sleep(time.Millisecond*1000)
        for i := 1; i <= 100000; i++ {
                time.Sleep(time.Millisecond*1)
                message := fmt.Sprintf("test message %d", i)
                err := client.SendMessage("TEST1", []byte(message))
                if err != nil {
                        log.Printf("send error %v", err.Error())
                }
        }
        time.Sleep(time.Millisecond*1000)
}

func producer() {
	cfg := nsq.NewConfig()
	nsqdAddr := "127.0.0.1:4150"
	producer, err := nsq.NewProducer(nsqdAddr, cfg)
	if err != nil {
		log.Fatal(err)
	}

	for i := 1; i <= 1000; i++ {
		message := fmt.Sprint("test message %v", i)
		if err := producer.Publish("test222", []byte(message)); err != nil {
			log.Fatal("publish error: " + err.Error())
		}
		time.Sleep(time.Second)
	}

}

type ProducerClient struct {
	lookUpAddrs []string
	sqs         chan string

	producerMap     map[string]*nsq.Producer
	producerMaplock sync.Mutex

	init bool
}

func NewProducerClient(lookUpAddrs []string) *ProducerClient {
	client := &ProducerClient{
		lookUpAddrs: lookUpAddrs,
		sqs:         make(chan string, 1024),
		producerMap: make(map[string]*nsq.Producer),
		init:        false,
	}
	go client.LoopLookUp()
	return client
}

func (client *ProducerClient) AddProducer(addr string, producer *nsq.Producer) {
	client.producerMaplock.Lock()
	defer client.producerMaplock.Unlock()
        log.Printf("add producer %v", addr)
	original, ok := client.producerMap[addr]
	if ok {
		original.Stop()
	}
	client.producerMap[addr] = producer
	client.init = true
}

func (client *ProducerClient) GetProducer(addr string) *nsq.Producer {
	client.producerMaplock.Lock()
	defer client.producerMaplock.Unlock()
	return client.producerMap[addr]
}

func (client *ProducerClient) GetRandomProducer() (string, *nsq.Producer) {
	client.producerMaplock.Lock()
	defer client.producerMaplock.Unlock()
	for addr, producer := range client.producerMap {
		return addr, producer
	}
	return "", nil
}

func (client *ProducerClient) DeleteProducer(addr string) {
	client.producerMaplock.Lock()
	defer client.producerMaplock.Unlock()
	producer, ok := client.producerMap[addr]
	if ok {
		producer.Stop()
	}
	delete(client.producerMap, addr)
}

func (client *ProducerClient) LoopLookUp() {
        client.queryLookupd()
	ticker := time.NewTicker(time.Second * 60)
	for {
		select {
		case <-ticker.C:
			client.queryLookupd()
		}
	}
}

func (client *ProducerClient) SendMessage(topic string, msg []byte) error {
	if !client.init {
		return fmt.Errorf("not init")
	}

	succ := false
	for {
		addr, producer := client.GetRandomProducer()
		if producer == nil {
			break
		}
		if err := producer.Publish(topic, msg); err != nil {
			log.Printf("addr %v publish error: %v ", addr, err.Error())
			client.DeleteProducer(addr)
			continue
		}else {
                        log.Printf("send message var:%v body:%v", addr, string(msg))
                }

		succ = true
		break
	}

	if succ {
		return nil
	}
	//发送失败的时候，都需要重新初始化
	client.init = false
	return fmt.Errorf("send failed")
}

func getLookUrl(addr string) string {
	url := "http://" + addr + "/nodes"
	return url
}

type Nodes struct {
	Producer []*ProducerNode `json:"producers"`
}

type ProducerNode struct {
	HostName string `json:"hostname"`
	TcpPort  int32  `json:"tcp_port"`
}

func (client *ProducerClient) queryLookupd() {
	for _, lookupAddr := range client.lookUpAddrs {
		nodeUrl := getLookUrl(lookupAddr)
		resp, err := http.Get(nodeUrl)
		if err != nil {
			log.Printf("lookup failed %v", err.Error())
			continue
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		log.Printf("lookup response:%v", string(body))
		nodes := &Nodes{Producer: make([]*ProducerNode, 0)}

		err = json.Unmarshal(body, nodes)
		if err != nil {
			log.Printf("lookup Unmarshal %v", err.Error())
			continue
		}
                log.Printf("nodes %v", nodes)
		for _, node := range nodes.Producer {
			addr := node.HostName + ":" + fmt.Sprintf("%d", node.TcpPort)
			if client.GetProducer(addr) != nil {
				continue
			}

			cfg := nsq.NewConfig()
			producer, err := nsq.NewProducer(addr, cfg)
                        if err != nil {
                                log.Printf("init producer error %v", err.Error())
                                continue

                        }
			client.AddProducer(addr, producer)

		}
                break
	}
}
