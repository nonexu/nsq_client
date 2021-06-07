package main

import (

        "log"
        "fmt"
        "github.com/nsqio/go-nsq"
)
func main() {
      consumer([]string{"127.0.0.1:4161", "127.0.0.1:4261"})
}

func consumer(NSQDsAddrs []string) {
        cfg := nsq.NewConfig()
        consumer, err := nsq.NewConsumer("TEST1", "TEST1", cfg)
        if err != nil {
                log.Fatal(err)
        }
        i := 0
        consumer.AddHandler(nsq.HandlerFunc(
                func(message *nsq.Message) error {
                        //log.Println(string(message.Body) + " C1")
                        i++
                        fmt.Println(i, message.NSQDAddress, string(message.Body))
                        return nil
                }))
        //ConnectToNSQDs        
        if err := consumer.ConnectToNSQLookupds(NSQDsAddrs); err != nil {
                log.Fatal(err, " C1")
        }
        <-consumer.StopChan
}