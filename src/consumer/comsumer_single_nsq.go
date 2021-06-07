package main

import (

        "log"
        "fmt"
        "github.com/nsqio/go-nsq"
)
func main() {
      consumer([]string{"127.0.0.1:4150"})  
}

func consumer(NSQDsAddrs []string) {
        cfg := nsq.NewConfig()
        consumer, err := nsq.NewConsumer("TEST1", "TEST1", cfg)
        if err != nil {
                log.Fatal(err)
        }
        consumer.AddHandler(nsq.HandlerFunc(
                func(message *nsq.Message) error {
                        //log.Println(string(message.Body) + " C1")
                        fmt.Println(string(message.Body))
                        return nil
                }))
        if err := consumer.ConnectToNSQDs(NSQDsAddrs); err != nil {
                log.Fatal(err, " C1")
        }
        <-consumer.StopChan
}