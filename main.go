package main

import (
	"bufio"
	"context"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)
var (
	wg sync.WaitGroup
)
type People struct {
	Cid string `json:"cid"`
	Uid string `json:"uid"`
}
func setUpLogger() {
	logFileLocation, _ := os.OpenFile("./log/test.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0744)
	log.SetOutput(logFileLocation)
}
func initClient() (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		Password: "",
		DB: 0,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	defer cancel()
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		return rdb, err
	}
	return rdb, nil
}
func addToRedis(lines []string) {
	wg.Add(1)
	var rdb *redis.Client
	var err error
	ctx := context.Background()
	if rdb, err = initClient(); err != nil {
		log.Printf("Redis connect error")
	}
	defer rdb.Close()
	for i := 0; i < len(lines); i++ {
		strs := strings.Split(lines[i], " ")
		rdb.SAdd(ctx, strs[0], strs[1])
	}
	wg.Done()
}
func matchCrowd(cid, uid string) bool {
	var err error
	var rdb *redis.Client
	ctx := context.Background()
	if rdb, err = initClient(); err != nil {
		log.Printf("Redis connect error")
	}
	defer rdb.Close()
	return rdb.SIsMember(ctx, cid, uid).Val()
}
func isMatch(c *gin.Context) {
	p := People{}
	c.ShouldBind(&p)
	c.String(http.StatusOK, "%t", matchCrowd(p.Cid, p.Uid))
}
func updateCrowd(cid, uid string) {
	var err error
	var rdb *redis.Client
	ctx := context.Background()
	if rdb, err = initClient(); err != nil {
		log.Printf("Redis connect error")
	}
	defer rdb.Close()
	rdb.SAdd(ctx, cid, uid)
}
func update(c *gin.Context) {
	p := People{}
	c.ShouldBind(&p)
	updateCrowd(p.Cid, p.Uid)
}
func main() {
	setUpLogger()
	r := gin.Default()
	file, err := os.Open("C:\\Users\\pantianyu\\Desktop\\test.txt")
	if err != nil {
		log.Printf("File open error")
	}
	defer file.Close()
	lines := []string{}
	iterator := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		iterator++
		if iterator <= 2000 {
			lines = append(lines, line)
		} else {
			iterator = 0
			lines = nil
			go addToRedis(lines)
		}
	}
	wg.Wait()
	r.POST("/matchCrowd", func (c *gin.Context) {
		isMatch(c)
	})
	r.POST("/updateCrowd", func (c *gin.Context) {
		update(c)
	})
	log.Printf("Server start succeed")
	r.Run(":8080")
}
