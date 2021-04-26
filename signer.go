package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

// сюда писать код
func ExecutePipeline(jobs ...job)  {
	in := make(chan interface{})
	wg := &sync.WaitGroup{}

	for _, j := range jobs {
		wg.Add(1)
		out := make(chan interface{})
		go doWork(j, in, out, wg)

		in = out
	}

	wg.Wait()
}

func doWork(job job, in, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(out)

	job(in, out)
}

func SingleHash(in chan interface{}, out chan interface{}) {
	mx := &sync.Mutex{}
	wg := &sync.WaitGroup{}

	for data := range in {
		wg.Add(1)
		go doSingleHash(data, out, wg, mx)
	}
	wg.Wait()

}

func doSingleHash(data interface{}, out chan interface{}, wg *sync.WaitGroup, mx *sync.Mutex) {
	defer wg.Done()

	parsedData := strconv.Itoa(data.(int))



	//To prevent overheat
	mx.Lock()
	md5Data := DataSignerMd5(parsedData)
	mx.Unlock()

	crc32Chan := make(chan string)
	go func(data string, out chan string) {
		defer close(out)
		out <- DataSignerCrc32(data)
	}(parsedData, crc32Chan)

	md5crc32Data := DataSignerCrc32(md5Data)
	crc32Data := <-crc32Chan

	out <- crc32Data + "~" + md5crc32Data
}



func MultiHash(in chan interface{}, out chan interface{}) {
	wg := &sync.WaitGroup{}

	for data := range in {
		wg.Add(1)
		go doMultiHash(data, out, wg)
	}

	wg.Wait()
}

func doMultiHash(data interface{}, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	const TH = 6

	newWg := &sync.WaitGroup{}

	res := make([]string, TH)
	parsedData := data.(string)

	for i := 0; i < TH; i++ {
		newWg.Add(1)

		iterData := strconv.Itoa(i)+parsedData
		go func(data string, wg  *sync.WaitGroup, res []string, i int) {

			defer wg.Done()
			res[i] = DataSignerCrc32(data)

		}(iterData, newWg, res, i)
	}

	newWg.Wait()

	out <- strings.Join(res, "")
}

func CombineResults(in chan interface{}, out chan interface{}) {
	var arr []string

	for data := range in {
		arr = append(arr, data.(string))
	}
	sort.Strings(arr)

	out <- strings.Join(arr, "_")
}