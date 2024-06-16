// Copyright 2020-2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// common

// --msgs 100000
// --size 128
// --no-progress
// --pubsleep 0s
// --subsleep 0s
// --multisubject
// --multisubjectmax 100000

// Common JS
// --purge
// --storage file
// --replicas 1
// --maxbytes 1GB
//
// nats bench pub
// nats bench sub
// nats bench request
// nats bench reply
// nats bench jspub
// nats bench jssub {ordered|consume|fetch}
// nats bench oldjssub {ordered|push|pull}
// nats bench kvput
// nats bench kvget

package cli

import (
	"bytes"
	"context"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/choria-io/fisk"
	"github.com/dustin/go-humanize"
	"github.com/gosuri/uiprogress"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/bench"
)

type benchCmd struct {
	subject              string
	numClients           int
	numPubs              int
	numSubs              int
	numMsg               int
	msgSizeString        string
	msgSize              int
	csvFile              string
	noProgress           bool
	request              bool
	reply                bool
	syncPub              bool
	pubBatch             int
	jsTimeout            time.Duration
	js                   bool
	storage              string
	streamName           string
	streamMaxBytesString string
	streamMaxBytes       int64
	consumerType         string
	ackMode              string
	pull                 bool
	push                 bool
	consumerBatch        int
	replicas             int
	purge                bool
	subSleep             time.Duration
	pubSleep             time.Duration
	consumerName         string
	kv                   bool
	bucketName           string
	history              uint8
	fetchTimeout         bool
	multiSubject         bool
	multiSubjectMax      int
	deDuplication        bool
	deDuplicationWindow  time.Duration
	retries              int
	retriesUsed          bool
	newJSAPI             bool
	ack                  bool
}

const (
	DefaultDurableConsumerName string = "natscli-bench"
	DefaultStreamName          string = "benchstream"
	DefaultBucketName          string = "benchbucket"
)

func configureBenchCommand(app commandHost) {
	c := &benchCmd{}

	benchHelp := `
Core NATS publish and subscribe:

  nats bench benchsubject --pub 1 --sub 10

Request reply with queue group:

  nats bench benchsubject --sub 1 --reply

  nats bench benchsubject --pub 10 --request

JetStream publish:

  nats bench benchsubject --js --purge --pub 1

JetStream ordered ephemeral consumers:

  nats bench benchsubject --js --sub 10

JetStream durable pull and push consumers:

  nats bench benchsubject --js --sub 5 --pull

  nats bench benchsubject --js --sub 5 --push

JetStream KV put and get:

  nats bench benchsubject --kv --pub 1

  nats bench benchsubject --kv --sub 10

Remember to use --no-progress to measure performance more accurately
`

	addCommonFlags := func(f *fisk.CmdClause) {
		f.Flag("clients", "Number of concurrent clients").Default("1").IntVar(&c.numClients)
		f.Flag("msgs", "Number of messages to publish or subscribe to").Default("100000").IntVar(&c.numMsg)
		f.Flag("no-progress", "Disable progress bar").UnNegatableBoolVar(&c.noProgress)
		f.Flag("csv", "Save benchmark data to CSV file").StringVar(&c.csvFile)
		f.Flag("size", "Size of the test messages").Default("128").StringVar(&c.msgSizeString)
		// TODO: support randomized payload data
	}

	addPubFlags := func(f *fisk.CmdClause) {
		f.Flag("pubsleep", "Sleep for the specified interval after publishing each message").Default("0s").DurationVar(&c.pubSleep)
		f.Flag("multisubject", "Multi-subject mode, each message is published on a subject that includes the publisher's message sequence number as a token").UnNegatableBoolVar(&c.multiSubject)
		f.Flag("multisubjectmax", "The maximum number of subjects to use in multi-subject mode (0 means no max)").Default("100000").IntVar(&c.multiSubjectMax)
	}

	addSubFlags := func(f *fisk.CmdClause) {
		f.Flag("multisubject", "Multi-subject mode, each message is published on a subject that includes the publisher's message sequence number as a token").UnNegatableBoolVar(&c.multiSubject)
		f.Flag("consumesleep", "Sleep for the specified interval before sending the subscriber acknowledgement back in --js mode, or sending the reply back in --reply mode,  or doing the next get in --kv mode").Default("0s").DurationVar(&c.subSleep)
	}

	addJSFlags := func(f *fisk.CmdClause) {
		f.Flag("purge", "Purge the stream before running").UnNegatableBoolVar(&c.purge)
		f.Flag("storage", "JetStream storage (memory/file) for the \"benchstream\" stream").Default("file").EnumVar(&c.storage, "memory", "file")
		f.Flag("replicas", "Number of stream replicas for the \"benchstream\" stream").Default("1").IntVar(&c.replicas)
		f.Flag("maxbytes", "The maximum size of the stream or KV bucket in bytes").Default("1GB").StringVar(&c.streamMaxBytesString)
		f.Flag("retries", "The maximum number of retries in JS operations").Default("3").IntVar(&c.retries)
		f.Flag("dedup", "Sets a message id in the header to use JS Publish de-duplication").Default("false").UnNegatableBoolVar(&c.deDuplication)
		f.Flag("dedupwindow", "Sets the duration of the stream's deduplication functionality").Default("2m").DurationVar(&c.deDuplicationWindow)
		f.Flag("js-timeout", "Timeout for JS operations").Default("30s").DurationVar(&c.jsTimeout)
	}

	addKVFlags := func(f *fisk.CmdClause) {
		f.Flag("purge", "Purge the stream before running").UnNegatableBoolVar(&c.purge)
		f.Flag("storage", "JetStream storage (memory/file) for the \"benchstream\" stream").Default("file").EnumVar(&c.storage, "memory", "file")
		f.Flag("replicas", "Number of stream replicas for the \"benchstream\" stream").Default("1").IntVar(&c.replicas)
		f.Flag("maxbytes", "The maximum size of the stream or KV bucket in bytes").Default("1GB").StringVar(&c.streamMaxBytesString)
		f.Flag("history", "History depth for the bucket in KV mode").Default("1").Uint8Var(&c.history)
		f.Flag("retries", "The maximum number of retries in JS operations").Default("3").IntVar(&c.retries)
	}

	benchCommand := app.Command("bench", "Benchmark utility")
	addCheat("bench", benchCommand)

	benchCommand.HelpLong(benchHelp)

	corePub := benchCommand.Command("pub", "Publish Core NATS messages").Action(c.pubAction)
	corePub.Arg("subject", "Subject to use for the benchmark").Required().StringVar(&c.subject)
	addCommonFlags(corePub)
	addPubFlags(corePub)

	coreSub := benchCommand.Command("sub", "Subscribe to Core NATS messages").Action(c.subAction)
	coreSub.Arg("subject", "Subject to use for the benchmark").Required().StringVar(&c.subject)
	addCommonFlags(coreSub)
	addSubFlags(coreSub)

	request := benchCommand.Command("request", "Request-Reply mode: send requests and wait for a reply").Action(c.requestAction)
	request.Arg("subject", "Subject to use for the benchmark").Required().StringVar(&c.subject)
	// TODO: support randomized payload data
	request.Flag("sleep", "Sleep for the specified interval between requests").Default("0s").DurationVar(&c.pubSleep)
	addCommonFlags(request)

	reply := benchCommand.Command("reply", "Request-Reply mode: receive requests and send a reply").Action(c.replyAction)
	reply.Arg("subject", "Subject to use for the benchmark").Required().StringVar(&c.subject)
	addCommonFlags(reply)

	jspub := benchCommand.Command("jspub", "Publish JetStream messages").Action(c.jspubAction)
	jspub.Arg("subject", "Subject to use for the benchmark").StringVar(&c.subject)
	jspub.Arg("stream", "The stream to consume from for the benchmark").Default(DefaultStreamName).StringVar(&c.streamName)
	jspub.Flag("async", "The number of asynchronous JS publish calls before waiting for the publish acknowledgements (set to 1 for synchronous)").Default("100").IntVar(&c.pubBatch)
	addCommonFlags(jspub)
	addPubFlags(jspub)
	addJSFlags(jspub)

	jsOrdered := benchCommand.Command("jsordered", "Consume JetStream messages from a consumer using an ephemeral ordered consumer").Action(c.jsOrderedAction)
	jsOrdered.Arg("stream", "The stream to consume from for the benchmark").Default(DefaultStreamName).StringVar(&c.streamName)
	jsOrdered.Flag("consumer", "Specify the durable consumer name to use").Default(DefaultDurableConsumerName).StringVar(&c.consumerName)
	jsOrdered.Flag("batch", "Sets the fetch or consume prefetch size").Default("100").IntVar(&c.consumerBatch)
	jsOrdered.Flag("ack", "Acknowledgement mode for the consumer").Default("explicit").EnumVar(&c.ackMode, "explicit", "none", "all")
	addCommonFlags(jsOrdered)
	addSubFlags(jsOrdered)
	addJSFlags(jsOrdered)

	jsConsume := benchCommand.Command("jsconsume", "Consume JetStream messages from a durable consumer using a callback").Action(c.jsConsumeAction)
	jsConsume.Arg("stream", "The stream to consume from for the benchmark").Default(DefaultStreamName).StringVar(&c.streamName)
	jsConsume.Flag("consumer", "Specify the durable consumer name to use").Default(DefaultDurableConsumerName).StringVar(&c.consumerName)
	jsConsume.Flag("batch", "Sets the fetch or consume prefetch size").Default("100").IntVar(&c.consumerBatch)
	jsConsume.Flag("ack", "Acknowledgement mode for the consumer").Default("explicit").EnumVar(&c.ackMode, "explicit", "none", "all")
	addCommonFlags(jsConsume)
	addSubFlags(jsConsume)
	addJSFlags(jsConsume)

	jsFetch := benchCommand.Command("jsfetch", "Consume JetStream messages from a durable consumer using fetch").Action(c.jsFetchAction)
	jsFetch.Arg("stream", "The stream to consume from for the benchmark").Default(DefaultStreamName).StringVar(&c.streamName)
	jsFetch.Flag("consumer", "Specify the durable consumer name to use").Default(DefaultDurableConsumerName).StringVar(&c.consumerName)
	jsFetch.Flag("batch", "Sets the fetch or consume prefetch size").Default("100").IntVar(&c.consumerBatch)
	jsFetch.Flag("ack", "Acknowledgement mode for the consumer").Default("explicit").EnumVar(&c.ackMode, "explicit", "none", "all")
	addCommonFlags(jsFetch)
	addSubFlags(jsFetch)
	addJSFlags(jsFetch)

	oldjsOrdered := benchCommand.Command("oldjsordered", "Consume JetStream messages from a consumer using the old API").Action(c.oldjsOrderedAction)
	oldjsOrdered.Arg("stream", "The stream to consumer from for the benchmark").Default(DefaultStreamName).StringVar(&c.streamName)
	oldjsOrdered.Arg("type", "Consumption method to use for the benchmark").EnumVar(&c.consumerType, "ordered", "push", "pull")
	oldjsOrdered.Flag("consumer", "Specify the durable consumer name to use").Default(DefaultDurableConsumerName).StringVar(&c.consumerName)
	oldjsOrdered.Flag("batch", "Sets the batch fetch size for the consumer for pull or the max ack pending value for push").Default("100").IntVar(&c.consumerBatch)
	oldjsOrdered.Flag("ack", "Uses explicit message acknowledgement or not for the push or pull consumer").Default("true").BoolVar(&c.ack)
	addCommonFlags(oldjsOrdered)
	addSubFlags(oldjsOrdered)
	addJSFlags(oldjsOrdered)

	oldjsPush := benchCommand.Command("oldjspush", "Consume JetStream messages from a consumer using the old API").Action(c.oldjsPushAction)
	oldjsPush.Arg("stream", "The stream to consumer from for the benchmark").Default(DefaultStreamName).StringVar(&c.streamName)
	oldjsPush.Arg("type", "Consumption method to use for the benchmark").EnumVar(&c.consumerType, "ordered", "push", "pull")
	oldjsPush.Flag("consumer", "Specify the durable consumer name to use").Default(DefaultDurableConsumerName).StringVar(&c.consumerName)
	oldjsPush.Flag("batch", "Sets the batch fetch size for the consumer for pull or the max ack pending value for push").Default("100").IntVar(&c.consumerBatch)
	oldjsPush.Flag("ack", "Uses explicit message acknowledgement or not for the push or pull consumer").Default("true").BoolVar(&c.ack)
	addCommonFlags(oldjsPush)
	addSubFlags(oldjsPush)
	addJSFlags(oldjsPush)

	oldjsPull := benchCommand.Command("oldjspull", "Consume JetStream messages from a consumer using the old API").Action(c.oldjsPullAction)
	oldjsPull.Arg("stream", "The stream to consumer from for the benchmark").Default(DefaultStreamName).StringVar(&c.streamName)
	oldjsPull.Arg("type", "Consumption method to use for the benchmark").EnumVar(&c.consumerType, "ordered", "push", "pull")
	oldjsPull.Flag("consumer", "Specify the durable consumer name to use").Default(DefaultDurableConsumerName).StringVar(&c.consumerName)
	oldjsPull.Flag("batch", "Sets the batch fetch size for the consumer for pull or the max ack pending value for push").Default("100").IntVar(&c.consumerBatch)
	oldjsPull.Flag("ack", "Uses explicit message acknowledgement or not for the push or pull consumer").Default("true").BoolVar(&c.ack)
	addCommonFlags(oldjsPull)
	addSubFlags(oldjsPull)
	addJSFlags(oldjsPull)

	kvput := benchCommand.Command("kvput", "Put messages in a KV bucket").Action(c.kvPutAction)
	kvput.Arg("bucket", "The bucket to use for the benchmark").Default(DefaultBucketName).StringVar(&c.bucketName)
	// TODO: support randomized payload data
	addCommonFlags(kvput)
	addKVFlags(kvput)

	kvget := benchCommand.Command("kvget", "Get messages from a KV bucket").Action(c.kvGetAction)
	kvget.Arg("bucket", "The bucket to use for the benchmark").Default(DefaultBucketName).StringVar(&c.bucketName)
	addCommonFlags(kvget)
	addKVFlags(kvget)
}

func init() {
	registerCommand("bench", 2, configureBenchCommand)
}

func (c *benchCmd) processActionArgs() error {
	if c.numMsg <= 0 {
		return fmt.Errorf("number of messages should be greater than 0")
	}

	// for pubs/request/and put only
	if c.msgSizeString != "" {
		msgSize, err := parseStringAsBytes(c.msgSizeString)
		if err != nil || msgSize <= 0 {
			return fmt.Errorf("can not parse or invalid the value specified for the message size: %s", c.msgSizeString)
		}

		c.msgSize = int(msgSize)
	}

	if opts().Config == nil {
		log.Fatalf("Unknown context %q", opts().CfgCtx)
	}

	if c.streamMaxBytesString != "" {
		size, err := parseStringAsBytes(c.streamMaxBytesString)
		if err != nil || size <= 0 {
			return fmt.Errorf("can not parse or invalid the value specified for the max stream/bucket size: %s", c.streamMaxBytesString)
		}

		c.streamMaxBytes = size
	}

	return nil
}

func (c *benchCmd) generateBanner(benchType string) string {
	// Create the banner which includes the appropriate argument names and values for the type of benchmark being run
	type nvp struct {
		name  string
		value string
	}

	var argnvps []nvp

	streamOrBucketAttribues := func() {
		if c.streamName == DefaultStreamName || c.bucketName == DefaultBucketName {
			argnvps = append(argnvps, nvp{"storage", c.storage})
			argnvps = append(argnvps, nvp{"max-bytes", f(uint64(c.streamMaxBytes))})
			argnvps = append(argnvps, nvp{"replicas", f(c.replicas)})
			argnvps = append(argnvps, nvp{"purge", f(c.purge)})
			argnvps = append(argnvps, nvp{"deduplication", f(c.deDuplication)})
			argnvps = append(argnvps, nvp{"dedup-window", f(c.deDuplicationWindow)})
		}
	}

	benchTypeLabel := "Unknown benchmark"

	switch benchType {
	case "pub":
		benchTypeLabel = "Core NATS publish"
		argnvps = append(argnvps, nvp{"subject", getSubscribeSubject(c)})
		argnvps = append(argnvps, nvp{"multi-subject", f(c.multiSubject)})
		argnvps = append(argnvps, nvp{"multi-subject-max", f(c.multiSubjectMax)})
		argnvps = append(argnvps, nvp{"pub-sleep", f(c.pubSleep)})
	case "sub":
		benchTypeLabel = "Core NATS subscribe"
		argnvps = append(argnvps, nvp{"subject", getSubscribeSubject(c)})
		argnvps = append(argnvps, nvp{"multi-subject", f(c.multiSubject)})
	case "request":
		benchTypeLabel = "Core NATS request"
		argnvps = append(argnvps, nvp{"subject", c.subject})
		argnvps = append(argnvps, nvp{"request-sleep", f(c.pubSleep)})
	case "reply":
		benchTypeLabel = "Core NATS reply"
		argnvps = append(argnvps, nvp{"subject", c.subject})
		argnvps = append(argnvps, nvp{"reply-sleep", f(c.subSleep)})
	case "jspub":
		benchTypeLabel = "JetStream publish"
		argnvps = append(argnvps, nvp{"subject", getSubscribeSubject(c)})
		argnvps = append(argnvps, nvp{"multi-subject", f(c.multiSubject)})
		argnvps = append(argnvps, nvp{"multi-subject-max", f(c.multiSubjectMax)})
		argnvps = append(argnvps, nvp{"pub-sleep", f(c.pubSleep)})
		argnvps = append(argnvps, nvp{"async", f(c.pubBatch)})
		streamOrBucketAttribues()
	case "jsordered":
		benchTypeLabel = "JetStream ordered ephemeral consumer"
		argnvps = append(argnvps, nvp{"stream", c.streamName})
		streamOrBucketAttribues()
	case "jsconsume":
		benchTypeLabel = "JetStream durable consume (callback)"
		argnvps = append(argnvps, nvp{"stream", c.streamName})
		argnvps = append(argnvps, nvp{"consumer", c.consumerName})
		streamOrBucketAttribues()
	case "jsfetch":
		benchTypeLabel = "JetStream durable consume (fetch)"
		argnvps = append(argnvps, nvp{"stream", c.streamName})
		argnvps = append(argnvps, nvp{"consumer", c.consumerName})
		streamOrBucketAttribues()
	case "oldjsordered":
		benchTypeLabel = "JetStream ordered ephemeral consumer (old API)"
		argnvps = append(argnvps, nvp{"stream", c.streamName})
		argnvps = append(argnvps, nvp{"consumer", c.consumerName})
		streamOrBucketAttribues()
	case "oldjspush":
		benchTypeLabel = "JetStream durable push consumer (old API)"
		argnvps = append(argnvps, nvp{"stream", c.streamName})
		argnvps = append(argnvps, nvp{"consumer", c.consumerName})
		streamOrBucketAttribues()
	case "oldjspull":
		benchTypeLabel = "JetStream durable pull consumer (old API)"
		argnvps = append(argnvps, nvp{"stream", c.streamName})
		argnvps = append(argnvps, nvp{"consumer", c.consumerName})
		streamOrBucketAttribues()
	case "kvput":
		benchTypeLabel = "KV put"
		argnvps = append(argnvps, nvp{"bucket", c.bucketName})
		argnvps = append(argnvps, nvp{"put-sleep", f(c.pubSleep)})
		streamOrBucketAttribues()
	case "kvget":
		benchTypeLabel = "KV get"
		argnvps = append(argnvps, nvp{"bucket", c.bucketName})
		streamOrBucketAttribues()
	}

	argnvps = append(argnvps, nvp{"msgs", f(c.numMsg)})
	argnvps = append(argnvps, nvp{"msg-size", humanize.IBytes(uint64(c.msgSize))})
	argnvps = append(argnvps, nvp{"clients", f(c.numClients)})

	if c.js {
		argnvps = append(argnvps, nvp{"pubbatch", f(c.pubBatch)})
		argnvps = append(argnvps, nvp{"jstimeout", f(c.jsTimeout)})
		argnvps = append(argnvps, nvp{"consumerbatch", f(c.consumerBatch)})
	}

	banner := fmt.Sprintf("Starting %s benchmark [", benchTypeLabel)

	var joinBuffer []string

	for _, v := range argnvps {
		joinBuffer = append(joinBuffer, v.name+"="+v.value)
	}
	banner += strings.Join(joinBuffer, ", ") + "]"

	return banner
}

func (c *benchCmd) printResults(bm *bench.Benchmark) error {
	if !c.noProgress {
		uiprogress.Stop()
	}

	if c.fetchTimeout {
		log.Print("WARNING: at least one of the pull consumer Fetch operation timed out. These results are not optimal!")
	}

	if c.retriesUsed {
		log.Print("WARNING: at least one of the JS publish operations had to be retried. These results are not optimal!")
	}

	fmt.Println()
	fmt.Println(bm.Report())

	if c.csvFile != "" {
		csv := bm.CSV()
		err := os.WriteFile(c.csvFile, []byte(csv), 0600)
		if err != nil {
			return fmt.Errorf("error writing file %s: %v", c.csvFile, err)
		}
		fmt.Printf("Saved metric data in csv file %s\n", c.csvFile)
	}

	return nil
}

func offset(putter int, counts []int) int {
	var position = 0

	for i := 0; i < putter; i++ {
		position = position + counts[i]
	}
	return position
}

func (c *benchCmd) storageType() jetstream.StorageType {
	switch c.storage {
	case "file":
		return jetstream.FileStorage
	case "memory":
		return jetstream.MemoryStorage
	default:
		{
			log.Printf("Unknown storage type %s, using memory", c.storage)
			return jetstream.MemoryStorage
		}
	}
}

func getjs(nc *nats.Conn) (jetstream.JetStream, error) {
	var err error
	var js jetstream.JetStream

	switch {
	case opts().JsDomain != "":
		js, err = jetstream.NewWithDomain(nc, opts().JsDomain)
	case opts().JsApiPrefix != "":
		js, err = jetstream.NewWithAPIPrefix(nc, opts().JsApiPrefix)
	default:
		js, err = jetstream.New(nc)
	}
	if err != nil {
		return nil, fmt.Errorf("couldn't get the new API JetStream instance: %v", err)
	}

	return js, nil
}

func (c *benchCmd) createOrCheckStream(js jetstream.JetStream) error {
	var s jetstream.Stream
	var err error

	ctx, cancel := context.WithTimeout(context.Background(), c.jsTimeout)
	defer cancel()

	if c.streamName == DefaultStreamName {
		// create the stream with our attributes, will create it if it doesn't exist or make sure the existing one has the same attributes
		s, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: c.streamName, Subjects: []string{c.subject}, Retention: jetstream.LimitsPolicy, Discard: jetstream.DiscardNew, Storage: jetstream.StorageType(c.storageType()), Replicas: c.replicas, MaxBytes: c.streamMaxBytes, Duplicates: c.deDuplicationWindow})
		if err != nil {
			return fmt.Errorf("%v. If you want to delete and re-define the stream use `nats stream delete %s`.", err, c.streamName)
		}
	} else {
		s, err = js.Stream(ctx, c.streamName)
		if err != nil {
			return fmt.Errorf("Stream %s does not exist: %v", c.streamName, err)
		}
		log.Printf("Using stream: %s", c.streamName)
	}

	if c.purge {
		log.Printf("Purging the stream")
		err = s.Purge(ctx)
		if err != nil {
			return fmt.Errorf("Error purging stream %s: %v", c.streamName, err)
		}
	}

	return nil
}

func (c *benchCmd) createOrUpdateConsumer(js jetstream.JetStream) error {
	ack := jetstream.AckNonePolicy
	if c.ack {
		ack = jetstream.AckExplicitPolicy
	}

	_, err := js.CreateOrUpdateConsumer(ctx, c.streamName, jetstream.ConsumerConfig{
		Durable:       c.consumerName,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     ack,
		ReplayPolicy:  jetstream.ReplayInstantPolicy,
		MaxAckPending: c.consumerBatch * c.numSubs,
	})
	if err != nil {
		fmt.Errorf("could not create the durable consumer %s: %v", c.consumerName, err)
	}

	return nil
}

// Actions for the various bench commands below
func (c *benchCmd) pubAction(_ *fisk.ParseContext) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner("pub")

	log.Println(banner)

	bm := bench.NewBenchmark("NATS", 0, c.numClients)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	pubCounts := bench.MsgsPerClient(c.numMsg, c.numClients)
	trigger := make(chan struct{})
	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runCorePublisher(bm, nc, startwg, donewg, trigger, pubCounts[i], offset(i, pubCounts), strconv.Itoa(i))
	}

	if !c.noProgress {
		uiprogress.Start()
	}

	startwg.Wait()
	close(trigger)
	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) subAction(_ *fisk.ParseContext) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner("sub")

	log.Println(banner)

	bm := bench.NewBenchmark("NATS", c.numClients, 0)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runCoreSubscriber(bm, nc, startwg, donewg, c.numMsg)
	}

	if !c.noProgress {
		uiprogress.Start()
	}

	startwg.Wait()
	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) requestAction(_ *fisk.ParseContext) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner("pub")

	log.Println(banner)

	bm := bench.NewBenchmark("NATS", 0, c.numClients)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	pubCounts := bench.MsgsPerClient(c.numMsg, c.numClients)
	trigger := make(chan struct{})
	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runCoreRequester(bm, nc, startwg, donewg, trigger, pubCounts[i], offset(i, pubCounts), strconv.Itoa(i))
	}

	if !c.noProgress {
		uiprogress.Start()
	}

	startwg.Wait()
	close(trigger)
	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) replyAction(_ *fisk.ParseContext) error {
	// reply mode is open-ended for the number of messages
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner("sub")

	log.Println(banner)

	bm := bench.NewBenchmark("NATS", c.numClients, 0)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runCoreReplier(bm, nc, startwg, donewg, c.numMsg)
	}

	startwg.Wait()
	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) jspubAction(_ *fisk.ParseContext) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner("jspub")
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", 0, c.numClients)
	benchId := strconv.FormatInt(time.Now().UnixMilli(), 16)
	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	// create the stream for the benchmark (and purge it)
	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		log.Fatalf("NATS connection failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.jsTimeout)
	defer cancel()

	var js2 jetstream.JetStream

	switch {
	case opts().JsDomain != "":
		js2, err = jetstream.NewWithDomain(nc, opts().JsDomain)
	case opts().JsApiPrefix != "":
		js2, err = jetstream.NewWithAPIPrefix(nc, opts().JsApiPrefix)
	default:
		js2, err = jetstream.New(nc)
	}
	if err != nil {
		log.Fatalf("Couldn't get the new API JetStream instance: %v", err)
	}

	var s jetstream.Stream
	if c.streamName == DefaultStreamName {
		// create the stream with our attributes, will create it if it doesn't exist or make sure the existing one has the same attributes
		s, err = js2.CreateStream(ctx, jetstream.StreamConfig{Name: c.streamName, Subjects: []string{getSubscribeSubject(c)}, Retention: jetstream.LimitsPolicy, Discard: jetstream.DiscardNew, Storage: jetstream.StorageType(c.storageType()), Replicas: c.replicas, MaxBytes: c.streamMaxBytes, Duplicates: c.deDuplicationWindow})
		if err != nil {
			log.Fatalf("%v. If you want to delete and re-define the stream use `nats stream delete %s`.", err, c.streamName)
		}
	} else {
		s, err = js2.Stream(ctx, c.streamName)
		if err != nil {
			log.Fatalf("Stream %s does not exist: %v", c.streamName, err)
		}
		log.Printf("Using stream: %s", c.streamName)
	}

	if c.purge {
		log.Printf("Purging the stream")
		err = s.Purge(ctx)
		if err != nil {
			log.Fatalf("Error purging stream %s: %v", c.streamName, err)
		}
	}

	pubCounts := bench.MsgsPerClient(c.numMsg, c.numClients)
	trigger := make(chan struct{})
	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runJSPublisher(bm, nc, startwg, donewg, trigger, pubCounts[i], offset(i, pubCounts), benchId, strconv.Itoa(i))
	}

	if !c.noProgress {
		uiprogress.Start()
	}

	startwg.Wait()
	close(trigger)
	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) jsOrderedAction(_ *fisk.ParseContext) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner("jsordered")
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", c.numClients, 0)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("could not establish NATS connection for client number %d: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runJSOrdered(bm, nc, startwg, donewg, c.numMsg)
	}
	startwg.Wait()

	if !c.noProgress {
		uiprogress.Start()
	}

	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) jsConsumeAction(_ *fisk.ParseContext) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner("jsconsume")
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", c.numClients, 0)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		return fmt.Errorf("could not establish NATS connection: %v", err)
	}
	defer nc.Close()

	js, err := getjs(nc)
	if err != nil {
		return err
	}

	if c.consumerName == DefaultDurableConsumerName {
		// create the consumer
		// TODO: Should it just delete and create each time?
		err = c.createOrUpdateConsumer(js)
		if err != nil {
			return err
		}

		defer func() {
			err := js.DeleteConsumer(ctx, c.streamName, c.consumerName)
			if err != nil {
				log.Fatalf("Error deleting the consumer on stream %s: %v", c.streamName, err)
			}
			log.Printf("Deleted durable consumer: %s\n", c.consumerName)
		}()
	}

	subCounts := bench.MsgsPerClient(c.numMsg, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("could not establish NATS connection for client number %d: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runJSConsume(bm, nc, startwg, donewg, subCounts[i], offset(i, subCounts))
	}
	startwg.Wait()

	if !c.noProgress {
		uiprogress.Start()
	}

	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) jsFetchAction(_ *fisk.ParseContext) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner("jsfetch")
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", c.numClients, 0)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		return fmt.Errorf("could not establish NATS connection: %v", err)
	}
	defer nc.Close()

	js, err := getjs(nc)
	if err != nil {
		return err
	}

	if c.consumerName == DefaultDurableConsumerName {
		// create the consumer
		// TODO: Should it be just create or delete and create each time?
		err = c.createOrUpdateConsumer(js)
		if err != nil {
			return err
		}

		defer func() {
			err := js.DeleteConsumer(ctx, c.streamName, c.consumerName)
			if err != nil {
				log.Fatalf("Error deleting the consumer on stream %s: %v", c.streamName, err)
			}
			log.Printf("Deleted durable consumer: %s\n", c.consumerName)
		}()
	}

	subCounts := bench.MsgsPerClient(c.numMsg, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("could not establish NATS connection for client number %d: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runJSFetch(bm, nc, startwg, donewg, subCounts[i], offset(i, subCounts))
	}
	startwg.Wait()

	if !c.noProgress {
		uiprogress.Start()
	}

	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) oldjsOrderedAction(_ *fisk.ParseContext) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner("jsordered")
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", c.numSubs, c.numPubs)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	var js nats.JetStreamContext

	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		log.Fatalf("NATS connection failed: %v", err)
	}

	js, err = nc.JetStream(append(jsOpts(), nats.MaxWait(c.jsTimeout))...)
	if err != nil {
		log.Fatalf("Couldn't get the JetStream context: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.jsTimeout)
	defer cancel()

	var js2 jetstream.JetStream

	switch {
	case opts().JsDomain != "":
		js2, err = jetstream.NewWithDomain(nc, opts().JsDomain)
	case opts().JsApiPrefix != "":
		js2, err = jetstream.NewWithAPIPrefix(nc, opts().JsApiPrefix)
	default:
		js2, err = jetstream.New(nc)
	}
	if err != nil {
		log.Fatalf("Couldn't get the new API JetStream instance: %v", err)
	}

	var s jetstream.Stream
	if c.streamName == DefaultStreamName {
		// create the stream with our attributes, will create it if it doesn't exist or make sure the existing one has the same attributes
		s, err = js2.CreateStream(ctx, jetstream.StreamConfig{Name: c.streamName, Subjects: []string{getSubscribeSubject(c)}, Retention: jetstream.LimitsPolicy, Discard: jetstream.DiscardNew, Storage: jetstream.StorageType(c.storageType()), Replicas: c.replicas, MaxBytes: c.streamMaxBytes, Duplicates: c.deDuplicationWindow})
		if err != nil {
			log.Fatalf("%v. If you want to delete and re-define the stream use `nats stream delete %s`.", err, c.streamName)
		}
	} else if (c.pull || c.push) && c.numSubs > 0 {
		s, err = js2.Stream(ctx, c.streamName)
		if err != nil {
			log.Fatalf("Stream %s does not exist: %v", c.streamName, err)
		}
		log.Printf("Using stream: %s", c.streamName)
	}

	if c.purge {
		log.Printf("Purging the stream")
		err = s.Purge(ctx)
		if err != nil {
			log.Fatalf("Error purging stream %s: %v", c.streamName, err)
		}
	}

	if c.numSubs > 0 {
		// create the simplified API consumer
		if c.newJSAPI {
			if c.pull || c.push {
				ack := jetstream.AckNonePolicy
				if c.ack {
					ack = jetstream.AckExplicitPolicy
				}

				_, err := js2.CreateOrUpdateConsumer(ctx, c.streamName, jetstream.ConsumerConfig{
					Durable:       c.consumerName,
					DeliverPolicy: jetstream.DeliverAllPolicy,
					AckPolicy:     ack,
					ReplayPolicy:  jetstream.ReplayInstantPolicy,
					MaxAckPending: c.consumerBatch * c.numSubs,
				})
				if err != nil {
					log.Fatalf("Error creating consumer %s: %v", c.consumerName, err)
				}
				defer func() {
					err := js2.DeleteConsumer(ctx, c.streamName, c.consumerName)
					if err != nil {
						log.Fatalf("Error deleting the consumer on stream %s: %v", c.streamName, err)
					}
					log.Printf("Deleted durable consumer: %s\n", c.consumerName)
				}()
			}
		} else if c.pull || c.push {
			ack := nats.AckNonePolicy
			if c.ack {
				ack = nats.AckExplicitPolicy
			}
			if c.pull && c.consumerName == DefaultDurableConsumerName {
				_, err = js.AddConsumer(c.streamName, &nats.ConsumerConfig{
					Durable:       c.consumerName,
					DeliverPolicy: nats.DeliverAllPolicy,
					AckPolicy:     ack,
					ReplayPolicy:  nats.ReplayInstantPolicy,
					MaxAckPending: func(a int) int {
						if a >= 10000 {
							return a
						} else {
							return 10000
						}
					}(c.numSubs * c.consumerBatch),
				})
				if err != nil {
					log.Fatalf("Error creating the pull consumer: %v", err)
				}
				defer func() {
					err := js.DeleteConsumer(c.streamName, c.consumerName)
					if err != nil {
						log.Printf("Error deleting the pull consumer on stream %s: %v", c.streamName, err)
					}
					log.Printf("Deleted durable consumer: %s\n", c.consumerName)
				}()
				log.Printf("Defined durable pull consumer: %s\n", c.consumerName)
			} else if c.push && c.consumerName == DefaultDurableConsumerName {
				ack := nats.AckNonePolicy
				if c.ack {
					ack = nats.AckExplicitPolicy
				}
				maxAckPending := 0
				if c.ack {
					maxAckPending = c.consumerBatch * c.numSubs
				}
				_, err = js.AddConsumer(c.streamName, &nats.ConsumerConfig{
					Durable:        c.consumerName,
					DeliverSubject: c.consumerName + "-DELIVERY",
					DeliverGroup:   c.consumerName + "-GROUP",
					DeliverPolicy:  nats.DeliverAllPolicy,
					AckPolicy:      ack,
					ReplayPolicy:   nats.ReplayInstantPolicy,
					MaxAckPending:  maxAckPending,
				})
				if err != nil {
					log.Fatal("Error creating the durable push consumer: ", err)
				}

				defer func() {
					err := js.DeleteConsumer(c.streamName, c.consumerName)
					if err != nil {
						log.Fatalf("Error deleting the durable push consumer on stream %s: %v", c.streamName, err)
					}
					log.Printf("Deleted durable consumer: %s\n", c.consumerName)
				}()
			}
			if c.ack {
				log.Printf("Defined durable explicitly acked push consumer: %s\n", c.consumerName)
			} else {
				log.Printf("Defined durable unacked push consumer: %s\n", c.consumerName)
			}
		}
	}

	subCounts := bench.MsgsPerClient(c.numMsg, c.numSubs)

	for i := 0; i < c.numSubs; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		numMsg := func() int {
			if c.pull || c.reply || c.push || c.kv {
				return subCounts[i]
			} else {
				return c.numMsg
			}
		}()

		go c.runSubscriber(bm, nc, startwg, donewg, numMsg, offset(i, subCounts))
	}
	startwg.Wait()

	if !c.noProgress {
		uiprogress.Start()
	}

	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) oldjsPushAction(_ *fisk.ParseContext) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner("jsordered")
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", c.numSubs, c.numPubs)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	var js nats.JetStreamContext

	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		log.Fatalf("NATS connection failed: %v", err)
	}

	js, err = nc.JetStream(append(jsOpts(), nats.MaxWait(c.jsTimeout))...)
	if err != nil {
		log.Fatalf("Couldn't get the JetStream context: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.jsTimeout)
	defer cancel()

	var js2 jetstream.JetStream

	switch {
	case opts().JsDomain != "":
		js2, err = jetstream.NewWithDomain(nc, opts().JsDomain)
	case opts().JsApiPrefix != "":
		js2, err = jetstream.NewWithAPIPrefix(nc, opts().JsApiPrefix)
	default:
		js2, err = jetstream.New(nc)
	}
	if err != nil {
		log.Fatalf("Couldn't get the new API JetStream instance: %v", err)
	}

	var s jetstream.Stream
	if c.streamName == DefaultStreamName {
		// create the stream with our attributes, will create it if it doesn't exist or make sure the existing one has the same attributes
		s, err = js2.CreateStream(ctx, jetstream.StreamConfig{Name: c.streamName, Subjects: []string{getSubscribeSubject(c)}, Retention: jetstream.LimitsPolicy, Discard: jetstream.DiscardNew, Storage: jetstream.StorageType(c.storageType()), Replicas: c.replicas, MaxBytes: c.streamMaxBytes, Duplicates: c.deDuplicationWindow})
		if err != nil {
			log.Fatalf("%v. If you want to delete and re-define the stream use `nats stream delete %s`.", err, c.streamName)
		}
	} else if (c.pull || c.push) && c.numSubs > 0 {
		s, err = js2.Stream(ctx, c.streamName)
		if err != nil {
			log.Fatalf("Stream %s does not exist: %v", c.streamName, err)
		}
		log.Printf("Using stream: %s", c.streamName)
	}

	if c.purge {
		log.Printf("Purging the stream")
		err = s.Purge(ctx)
		if err != nil {
			log.Fatalf("Error purging stream %s: %v", c.streamName, err)
		}
	}

	if c.numSubs > 0 {
		// create the simplified API consumer
		if c.newJSAPI {
			if c.pull || c.push {
				ack := jetstream.AckNonePolicy
				if c.ack {
					ack = jetstream.AckExplicitPolicy
				}

				_, err := js2.CreateOrUpdateConsumer(ctx, c.streamName, jetstream.ConsumerConfig{
					Durable:       c.consumerName,
					DeliverPolicy: jetstream.DeliverAllPolicy,
					AckPolicy:     ack,
					ReplayPolicy:  jetstream.ReplayInstantPolicy,
					MaxAckPending: c.consumerBatch * c.numSubs,
				})
				if err != nil {
					log.Fatalf("Error creating consumer %s: %v", c.consumerName, err)
				}
				defer func() {
					err := js2.DeleteConsumer(ctx, c.streamName, c.consumerName)
					if err != nil {
						log.Fatalf("Error deleting the consumer on stream %s: %v", c.streamName, err)
					}
					log.Printf("Deleted durable consumer: %s\n", c.consumerName)
				}()
			}
		} else if c.pull || c.push {
			ack := nats.AckNonePolicy
			if c.ack {
				ack = nats.AckExplicitPolicy
			}
			if c.pull && c.consumerName == DefaultDurableConsumerName {
				_, err = js.AddConsumer(c.streamName, &nats.ConsumerConfig{
					Durable:       c.consumerName,
					DeliverPolicy: nats.DeliverAllPolicy,
					AckPolicy:     ack,
					ReplayPolicy:  nats.ReplayInstantPolicy,
					MaxAckPending: func(a int) int {
						if a >= 10000 {
							return a
						} else {
							return 10000
						}
					}(c.numSubs * c.consumerBatch),
				})
				if err != nil {
					log.Fatalf("Error creating the pull consumer: %v", err)
				}
				defer func() {
					err := js.DeleteConsumer(c.streamName, c.consumerName)
					if err != nil {
						log.Printf("Error deleting the pull consumer on stream %s: %v", c.streamName, err)
					}
					log.Printf("Deleted durable consumer: %s\n", c.consumerName)
				}()
				log.Printf("Defined durable pull consumer: %s\n", c.consumerName)
			} else if c.push && c.consumerName == DefaultDurableConsumerName {
				ack := nats.AckNonePolicy
				if c.ack {
					ack = nats.AckExplicitPolicy
				}
				maxAckPending := 0
				if c.ack {
					maxAckPending = c.consumerBatch * c.numSubs
				}
				_, err = js.AddConsumer(c.streamName, &nats.ConsumerConfig{
					Durable:        c.consumerName,
					DeliverSubject: c.consumerName + "-DELIVERY",
					DeliverGroup:   c.consumerName + "-GROUP",
					DeliverPolicy:  nats.DeliverAllPolicy,
					AckPolicy:      ack,
					ReplayPolicy:   nats.ReplayInstantPolicy,
					MaxAckPending:  maxAckPending,
				})
				if err != nil {
					log.Fatal("Error creating the durable push consumer: ", err)
				}

				defer func() {
					err := js.DeleteConsumer(c.streamName, c.consumerName)
					if err != nil {
						log.Fatalf("Error deleting the durable push consumer on stream %s: %v", c.streamName, err)
					}
					log.Printf("Deleted durable consumer: %s\n", c.consumerName)
				}()
			}
			if c.ack {
				log.Printf("Defined durable explicitly acked push consumer: %s\n", c.consumerName)
			} else {
				log.Printf("Defined durable unacked push consumer: %s\n", c.consumerName)
			}
		}
	}

	subCounts := bench.MsgsPerClient(c.numMsg, c.numSubs)

	for i := 0; i < c.numSubs; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		numMsg := func() int {
			if c.pull || c.reply || c.push || c.kv {
				return subCounts[i]
			} else {
				return c.numMsg
			}
		}()

		go c.runSubscriber(bm, nc, startwg, donewg, numMsg, offset(i, subCounts))
	}
	startwg.Wait()

	if !c.noProgress {
		uiprogress.Start()
	}

	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) oldjsPullAction(_ *fisk.ParseContext) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner("jsordered")
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", c.numSubs, c.numPubs)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	var js nats.JetStreamContext

	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		log.Fatalf("NATS connection failed: %v", err)
	}

	js, err = nc.JetStream(append(jsOpts(), nats.MaxWait(c.jsTimeout))...)
	if err != nil {
		log.Fatalf("Couldn't get the JetStream context: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.jsTimeout)
	defer cancel()

	var js2 jetstream.JetStream

	switch {
	case opts().JsDomain != "":
		js2, err = jetstream.NewWithDomain(nc, opts().JsDomain)
	case opts().JsApiPrefix != "":
		js2, err = jetstream.NewWithAPIPrefix(nc, opts().JsApiPrefix)
	default:
		js2, err = jetstream.New(nc)
	}
	if err != nil {
		log.Fatalf("Couldn't get the new API JetStream instance: %v", err)
	}

	var s jetstream.Stream
	if c.streamName == DefaultStreamName {
		// create the stream with our attributes, will create it if it doesn't exist or make sure the existing one has the same attributes
		s, err = js2.CreateStream(ctx, jetstream.StreamConfig{Name: c.streamName, Subjects: []string{getSubscribeSubject(c)}, Retention: jetstream.LimitsPolicy, Discard: jetstream.DiscardNew, Storage: jetstream.StorageType(c.storageType()), Replicas: c.replicas, MaxBytes: c.streamMaxBytes, Duplicates: c.deDuplicationWindow})
		if err != nil {
			log.Fatalf("%v. If you want to delete and re-define the stream use `nats stream delete %s`.", err, c.streamName)
		}
	} else if (c.pull || c.push) && c.numSubs > 0 {
		s, err = js2.Stream(ctx, c.streamName)
		if err != nil {
			log.Fatalf("Stream %s does not exist: %v", c.streamName, err)
		}
		log.Printf("Using stream: %s", c.streamName)
	}

	if c.purge {
		log.Printf("Purging the stream")
		err = s.Purge(ctx)
		if err != nil {
			log.Fatalf("Error purging stream %s: %v", c.streamName, err)
		}
	}

	if c.numSubs > 0 {
		// create the simplified API consumer
		if c.newJSAPI {
			if c.pull || c.push {
				ack := jetstream.AckNonePolicy
				if c.ack {
					ack = jetstream.AckExplicitPolicy
				}

				_, err := js2.CreateOrUpdateConsumer(ctx, c.streamName, jetstream.ConsumerConfig{
					Durable:       c.consumerName,
					DeliverPolicy: jetstream.DeliverAllPolicy,
					AckPolicy:     ack,
					ReplayPolicy:  jetstream.ReplayInstantPolicy,
					MaxAckPending: c.consumerBatch * c.numSubs,
				})
				if err != nil {
					log.Fatalf("Error creating consumer %s: %v", c.consumerName, err)
				}
				defer func() {
					err := js2.DeleteConsumer(ctx, c.streamName, c.consumerName)
					if err != nil {
						log.Fatalf("Error deleting the consumer on stream %s: %v", c.streamName, err)
					}
					log.Printf("Deleted durable consumer: %s\n", c.consumerName)
				}()
			}
		} else if c.pull || c.push {
			ack := nats.AckNonePolicy
			if c.ack {
				ack = nats.AckExplicitPolicy
			}
			if c.pull && c.consumerName == DefaultDurableConsumerName {
				_, err = js.AddConsumer(c.streamName, &nats.ConsumerConfig{
					Durable:       c.consumerName,
					DeliverPolicy: nats.DeliverAllPolicy,
					AckPolicy:     ack,
					ReplayPolicy:  nats.ReplayInstantPolicy,
					MaxAckPending: func(a int) int {
						if a >= 10000 {
							return a
						} else {
							return 10000
						}
					}(c.numSubs * c.consumerBatch),
				})
				if err != nil {
					log.Fatalf("Error creating the pull consumer: %v", err)
				}
				defer func() {
					err := js.DeleteConsumer(c.streamName, c.consumerName)
					if err != nil {
						log.Printf("Error deleting the pull consumer on stream %s: %v", c.streamName, err)
					}
					log.Printf("Deleted durable consumer: %s\n", c.consumerName)
				}()
				log.Printf("Defined durable pull consumer: %s\n", c.consumerName)
			} else if c.push && c.consumerName == DefaultDurableConsumerName {
				ack := nats.AckNonePolicy
				if c.ack {
					ack = nats.AckExplicitPolicy
				}
				maxAckPending := 0
				if c.ack {
					maxAckPending = c.consumerBatch * c.numSubs
				}
				_, err = js.AddConsumer(c.streamName, &nats.ConsumerConfig{
					Durable:        c.consumerName,
					DeliverSubject: c.consumerName + "-DELIVERY",
					DeliverGroup:   c.consumerName + "-GROUP",
					DeliverPolicy:  nats.DeliverAllPolicy,
					AckPolicy:      ack,
					ReplayPolicy:   nats.ReplayInstantPolicy,
					MaxAckPending:  maxAckPending,
				})
				if err != nil {
					log.Fatal("Error creating the durable push consumer: ", err)
				}

				defer func() {
					err := js.DeleteConsumer(c.streamName, c.consumerName)
					if err != nil {
						log.Fatalf("Error deleting the durable push consumer on stream %s: %v", c.streamName, err)
					}
					log.Printf("Deleted durable consumer: %s\n", c.consumerName)
				}()
			}
			if c.ack {
				log.Printf("Defined durable explicitly acked push consumer: %s\n", c.consumerName)
			} else {
				log.Printf("Defined durable unacked push consumer: %s\n", c.consumerName)
			}
		}
	}

	subCounts := bench.MsgsPerClient(c.numMsg, c.numSubs)

	for i := 0; i < c.numSubs; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		numMsg := func() int {
			if c.pull || c.reply || c.push || c.kv {
				return subCounts[i]
			} else {
				return c.numMsg
			}
		}()

		go c.runSubscriber(bm, nc, startwg, donewg, numMsg, offset(i, subCounts))
	}
	startwg.Wait()

	if !c.noProgress {
		uiprogress.Start()
	}

	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) kvPutAction(_ *fisk.ParseContext) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner("kvput")
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", 0, c.numClients)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		return fmt.Errorf("could not establish NATS connection : %v", err)
	}
	defer nc.Close()

	js, err := getjs(nc)
	if err != nil {
		return err
	}

	// There is no way to purge all the keys in a KV bucket in a single operation so deleting the bucket instead
	if c.purge {
		err = js.DeleteStream(ctx, "KV_"+c.bucketName)
		// err = js.DeleteKeyValue(c.subject)
		if err != nil {
			log.Fatalf("Error trying to purge the bucket: %v", err)
		}
	}

	if c.bucketName == DefaultBucketName {
		// create bucket
		_, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: c.bucketName, History: c.history, Storage: c.storageType(), Description: "nats bench bucket", Replicas: c.replicas, MaxBytes: c.streamMaxBytes})
		if err != nil {
			log.Fatalf("Couldn't create the KV bucket: %v", err)
		}
	}

	startwg.Wait()

	pubCounts := bench.MsgsPerClient(c.numMsg, c.numClients)
	trigger := make(chan struct{})
	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runKVPutter(bm, nc, startwg, donewg, trigger, pubCounts[i], strconv.Itoa(i), offset(i, pubCounts))
	}

	if !c.noProgress {
		uiprogress.Start()
	}

	startwg.Wait()
	close(trigger)
	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) kvGetAction(_ *fisk.ParseContext) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner("kvput")
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", 0, c.numClients)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	subCounts := bench.MsgsPerClient(c.numMsg, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runKVGetter(bm, nc, startwg, donewg, subCounts[i], offset(i, subCounts))
	}
	startwg.Wait()

	if !c.noProgress {
		uiprogress.Start()
	}

	startwg.Wait()
	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) bench(_ *fisk.ParseContext) error {
	// first check the sanity of the arguments
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner("bench")
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", c.numSubs, c.numPubs)
	benchId := strconv.FormatInt(time.Now().UnixMilli(), 16)
	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	var js nats.JetStreamContext

	storageType := func() nats.StorageType {
		switch c.storage {
		case "file":
			return nats.FileStorage
		case "memory":
			return nats.MemoryStorage
		default:
			{
				log.Printf("Unknown storage type %s, using memory", c.storage)
				return nats.MemoryStorage
			}
		}
	}()

	if c.js || c.kv {
		// create the stream for the benchmark (and purge it)
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			log.Fatalf("NATS connection failed: %v", err)
		}

		js, err = nc.JetStream(append(jsOpts(), nats.MaxWait(c.jsTimeout))...)
		if err != nil {
			log.Fatalf("Couldn't get the JetStream context: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), c.jsTimeout)
		defer cancel()

		var js2 jetstream.JetStream

		switch {
		case opts().JsDomain != "":
			js2, err = jetstream.NewWithDomain(nc, opts().JsDomain)
		case opts().JsApiPrefix != "":
			js2, err = jetstream.NewWithAPIPrefix(nc, opts().JsApiPrefix)
		default:
			js2, err = jetstream.New(nc)
		}
		if err != nil {
			log.Fatalf("Couldn't get the new API JetStream instance: %v", err)
		}

		if c.kv {

			// There is no way to purge all the keys in a KV bucket in a single operation so deleting the bucket instead
			if c.purge {
				err = js2.DeleteStream(ctx, "KV_"+c.bucketName)
				// err = js.DeleteKeyValue(c.subject)
				if err != nil {
					log.Fatalf("Error trying to purge the bucket: %v", err)
				}
			}

			if c.bucketName == DefaultBucketName {
				// create bucket
				_, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: c.bucketName, History: c.history, Storage: storageType, Description: "nats bench bucket", Replicas: c.replicas, MaxBytes: c.streamMaxBytes})
				if err != nil {
					log.Fatalf("Couldn't create the KV bucket: %v", err)
				}
			}
		} else if c.js {
			var s jetstream.Stream
			if c.streamName == DefaultStreamName {
				// create the stream with our attributes, will create it if it doesn't exist or make sure the existing one has the same attributes
				s, err = js2.CreateStream(ctx, jetstream.StreamConfig{Name: c.streamName, Subjects: []string{getSubscribeSubject(c)}, Retention: jetstream.LimitsPolicy, Discard: jetstream.DiscardNew, Storage: jetstream.StorageType(storageType), Replicas: c.replicas, MaxBytes: c.streamMaxBytes, Duplicates: c.deDuplicationWindow})
				if err != nil {
					log.Fatalf("%v. If you want to delete and re-define the stream use `nats stream delete %s`.", err, c.streamName)
				}
			} else if (c.pull || c.push) && c.numSubs > 0 {
				s, err = js2.Stream(ctx, c.streamName)
				if err != nil {
					log.Fatalf("Stream %s does not exist: %v", c.streamName, err)
				}
				log.Printf("Using stream: %s", c.streamName)
			}

			if c.purge {
				log.Printf("Purging the stream")
				err = s.Purge(ctx)
				if err != nil {
					log.Fatalf("Error purging stream %s: %v", c.streamName, err)
				}
			}

			if c.numSubs > 0 {
				// create the simplified API consumer
				if c.newJSAPI {
					if c.pull || c.push {
						ack := jetstream.AckNonePolicy
						if c.ack {
							ack = jetstream.AckExplicitPolicy
						}

						_, err := js2.CreateOrUpdateConsumer(ctx, c.streamName, jetstream.ConsumerConfig{
							Durable:       c.consumerName,
							DeliverPolicy: jetstream.DeliverAllPolicy,
							AckPolicy:     ack,
							ReplayPolicy:  jetstream.ReplayInstantPolicy,
							MaxAckPending: c.consumerBatch * c.numSubs,
						})
						if err != nil {
							log.Fatalf("Error creating consumer %s: %v", c.consumerName, err)
						}
						defer func() {
							err := js2.DeleteConsumer(ctx, c.streamName, c.consumerName)
							if err != nil {
								log.Fatalf("Error deleting the consumer on stream %s: %v", c.streamName, err)
							}
							log.Printf("Deleted durable consumer: %s\n", c.consumerName)
						}()
					}
				} else if c.pull || c.push {
					ack := nats.AckNonePolicy
					if c.ack {
						ack = nats.AckExplicitPolicy
					}
					if c.pull && c.consumerName == DefaultDurableConsumerName {
						_, err = js.AddConsumer(c.streamName, &nats.ConsumerConfig{
							Durable:       c.consumerName,
							DeliverPolicy: nats.DeliverAllPolicy,
							AckPolicy:     ack,
							ReplayPolicy:  nats.ReplayInstantPolicy,
							MaxAckPending: func(a int) int {
								if a >= 10000 {
									return a
								} else {
									return 10000
								}
							}(c.numSubs * c.consumerBatch),
						})
						if err != nil {
							log.Fatalf("Error creating the pull consumer: %v", err)
						}
						defer func() {
							err := js.DeleteConsumer(c.streamName, c.consumerName)
							if err != nil {
								log.Printf("Error deleting the pull consumer on stream %s: %v", c.streamName, err)
							}
							log.Printf("Deleted durable consumer: %s\n", c.consumerName)
						}()
						log.Printf("Defined durable pull consumer: %s\n", c.consumerName)
					} else if c.push && c.consumerName == DefaultDurableConsumerName {
						ack := nats.AckNonePolicy
						if c.ack {
							ack = nats.AckExplicitPolicy
						}
						maxAckPending := 0
						if c.ack {
							maxAckPending = c.consumerBatch * c.numSubs
						}
						_, err = js.AddConsumer(c.streamName, &nats.ConsumerConfig{
							Durable:        c.consumerName,
							DeliverSubject: c.consumerName + "-DELIVERY",
							DeliverGroup:   c.consumerName + "-GROUP",
							DeliverPolicy:  nats.DeliverAllPolicy,
							AckPolicy:      ack,
							ReplayPolicy:   nats.ReplayInstantPolicy,
							MaxAckPending:  maxAckPending,
						})
						if err != nil {
							log.Fatal("Error creating the durable push consumer: ", err)
						}

						defer func() {
							err := js.DeleteConsumer(c.streamName, c.consumerName)
							if err != nil {
								log.Fatalf("Error deleting the durable push consumer on stream %s: %v", c.streamName, err)
							}
							log.Printf("Deleted durable consumer: %s\n", c.consumerName)
						}()
					}
					if c.ack {
						log.Printf("Defined durable explicitly acked push consumer: %s\n", c.consumerName)
					} else {
						log.Printf("Defined durable unacked push consumer: %s\n", c.consumerName)
					}
				}
			}
		}
	}

	subCounts := bench.MsgsPerClient(c.numMsg, c.numSubs)

	for i := 0; i < c.numSubs; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		numMsg := func() int {
			if c.pull || c.reply || c.push || c.kv {
				return subCounts[i]
			} else {
				return c.numMsg
			}
		}()

		go c.runSubscriber(bm, nc, startwg, donewg, numMsg, offset(i, subCounts))
	}
	startwg.Wait()

	pubCounts := bench.MsgsPerClient(c.numMsg, c.numPubs)
	trigger := make(chan struct{})
	for i := 0; i < c.numPubs; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runPublisher(bm, nc, startwg, donewg, trigger, pubCounts[i], offset(i, pubCounts), benchId, strconv.Itoa(i))
	}

	if !c.noProgress {
		uiprogress.Start()
	}

	startwg.Wait()
	close(trigger)
	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func getSubscribeSubject(c *benchCmd) string {
	if c.multiSubject {
		return c.subject + ".*"
	} else {
		return c.subject
	}
}

func getPublishSubject(c *benchCmd, number int) string {
	if c.multiSubject {
		if c.multiSubjectMax == 0 {
			return c.subject + "." + strconv.Itoa(number)
		} else {
			return c.subject + "." + strconv.Itoa(number%c.multiSubjectMax)
		}
	} else {
		return c.subject
	}
}

func coreNATSPublisher(c benchCmd, nc *nats.Conn, progress *uiprogress.Bar, msg []byte, numMsg int, offset int) {
	state := "Publishing"

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	for i := 0; i < numMsg; i++ {
		if progress != nil {
			progress.Incr()
		}

		err := nc.Publish(getPublishSubject(&c, i+offset), msg)
		if err != nil {
			log.Fatalf("Publish error: %v", err)
		}

		time.Sleep(c.pubSleep)
	}

	state = "Finished  "
}

func coreNATSRequester(c benchCmd, nc *nats.Conn, progress *uiprogress.Bar, msg []byte, numMsg int, offset int) {
	errBytes := []byte("error")
	minusByte := byte('-')

	state := "Requesting"

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	for i := 0; i < numMsg; i++ {
		if progress != nil {
			progress.Incr()
		}

		m, err := nc.Request(getPublishSubject(&c, i+offset), msg, time.Second)
		if err != nil {
			log.Fatalf("Request error %v", err)
		}

		if len(m.Data) == 0 || m.Data[0] == minusByte || bytes.Contains(m.Data, errBytes) {
			log.Fatalf("Request did not receive a good reply: %q", m.Data)
		}

		time.Sleep(c.pubSleep)
	}

	state = "Finished  "
}

func jsPublisher(c *benchCmd, nc *nats.Conn, progress *uiprogress.Bar, msg []byte, numMsg int, idPrefix string, pubNumber string, offset int) {
	js, err := nc.JetStream(jsOpts()...)
	if err != nil {
		log.Fatalf("Couldn't get the JetStream context: %v", err)
	}

	var state string

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	if !c.syncPub {
		for i := 0; i < numMsg; {
			state = "Publishing"
			futures := make([]nats.PubAckFuture, min(c.pubBatch, numMsg-i))
			for j := 0; j < c.pubBatch && (i+j) < numMsg; j++ {
				if c.deDuplication {
					header := nats.Header{}
					header.Set(nats.MsgIdHdr, idPrefix+"-"+pubNumber+"-"+strconv.Itoa(i+j+offset))
					message := nats.Msg{Data: msg, Header: header, Subject: getPublishSubject(c, i+j+offset)}
					futures[j], err = js.PublishMsgAsync(&message)
				} else {
					futures[j], err = js.PublishAsync(getPublishSubject(c, i+j+offset), msg)
				}
				if err != nil {
					log.Fatalf("PubAsync error: %v", err)
				}
				if progress != nil {
					progress.Incr()
				}
				time.Sleep(c.pubSleep)
			}

			state = "AckWait   "

			select {
			case <-js.PublishAsyncComplete():
				state = "ProcessAck"
				for future := range futures {
					select {
					case <-futures[future].Ok():
						i++
					case err := <-futures[future].Err():
						if err.Error() == "nats: maximum bytes exceeded" {
							log.Fatalf("Stream maximum bytes exceeded, can not publish any more messages")
						}
						log.Printf("PubAsyncFuture for message %v in batch not OK: %v (retrying)", future, err)
						c.retriesUsed = true
					}
				}
			case <-time.After(c.jsTimeout):
				c.retriesUsed = true
				log.Printf("JS PubAsync ack timeout (pending=%d)", js.PublishAsyncPending())
				js, err = nc.JetStream(jsOpts()...)
				if err != nil {
					log.Fatalf("Couldn't get the JetStream context: %v", err)
				}
			}
		}
		state = "Finished  "
	} else {
		state = "Publishing"
		for i := 0; i < numMsg; i++ {
			if progress != nil {
				progress.Incr()
			}
			if c.deDuplication {
				header := nats.Header{}
				header.Set(nats.MsgIdHdr, idPrefix+"-"+pubNumber+"-"+strconv.Itoa(i+offset))
				message := nats.Msg{Data: msg, Header: header, Subject: getPublishSubject(c, i+offset)}
				_, err = js.PublishMsg(&message)
			} else {
				_, err = js.Publish(getPublishSubject(c, i+offset), msg)
			}
			if err != nil {
				if err.Error() == "nats: maximum bytes exceeded" {
					log.Fatalf("Stream maximum bytes exceeded, can not publish any more messages")
				}
				log.Printf("Publish error: %v (retrying)", err)
				c.retriesUsed = true
				i--
			}
			time.Sleep(c.pubSleep)
		}
	}
}

func kvPutter(c benchCmd, nc *nats.Conn, progress *uiprogress.Bar, msg []byte, numMsg int, offset int) {
	js, err := nc.JetStream(jsOpts()...)
	if err != nil {
		log.Fatalf("Couldn't get the JetStream context: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.jsTimeout)
	defer cancel()

	var js2 jetstream.JetStream

	switch {
	case opts().JsDomain != "":
		js2, err = jetstream.NewWithDomain(nc, opts().JsDomain)
	case opts().JsApiPrefix != "":
		js2, err = jetstream.NewWithAPIPrefix(nc, opts().JsApiPrefix)
	default:
		js2, err = jetstream.New(nc)
	}
	if err != nil {
		log.Fatalf("Couldn't get the new API JetStream instance: %v", err)
	}

	if c.newJSAPI {
		kvBucket, err := js2.KeyValue(ctx, c.bucketName)
		if err != nil {
			log.Fatalf("Couldn't find kv bucket %s: %v", c.bucketName, err)
		}
		var state string = "Putting   "

		if progress != nil {
			progress.PrependFunc(func(b *uiprogress.Bar) string {
				return state
			})
		}

		for i := 0; i < numMsg; i++ {
			if progress != nil {
				progress.Incr()
			}

			_, err = kvBucket.Put(ctx, fmt.Sprintf("%d", offset+i), msg)
			if err != nil {
				log.Fatalf("Put: %s", err)
			}

			time.Sleep(c.pubSleep)
		}
	} else {
		kvBucket, err := js.KeyValue(c.bucketName)
		if err != nil {
			log.Fatalf("Couldn't find kv bucket %s: %v", c.bucketName, err)
		}

		var state string = "Putting   "

		if progress != nil {
			progress.PrependFunc(func(b *uiprogress.Bar) string {
				return state
			})
		}

		for i := 0; i < numMsg; i++ {
			if progress != nil {
				progress.Incr()
			}

			_, err = kvBucket.Put(fmt.Sprintf("%d", offset+i), msg)
			if err != nil {
				log.Fatalf("Put: %s", err)
			}

			time.Sleep(c.pubSleep)
		}
	}

}

func (c *benchCmd) runPublisher(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, trigger chan struct{}, numMsg int, offset int, idPrefix string, pubNumber string) {
	startwg.Done()

	var progress *uiprogress.Bar

	if c.kv {
		log.Printf("Starting KV putter, putting %s messages", f(numMsg))
	} else {
		log.Printf("Starting publisher, publishing %s messages", f(numMsg))
	}

	if !c.noProgress {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = progressWidth()
	}

	var msg []byte
	if c.msgSize > 0 {
		msg = make([]byte, c.msgSize)
	}

	<-trigger

	// introduces some jitter between the publishers if pubSleep is set and more than one publisher
	if c.pubSleep != 0 && pubNumber != "0" {
		n := rand.Intn(int(c.pubSleep))
		time.Sleep(time.Duration(n))
	}

	start := time.Now()

	if !c.js && !c.kv {
		coreNATSPublisher(*c, nc, progress, msg, numMsg, offset)
	} else if c.kv {
		kvPutter(*c, nc, progress, msg, numMsg, offset)
	} else if c.js {
		jsPublisher(c, nc, progress, msg, numMsg, idPrefix, pubNumber, offset)
	}

	err := nc.Flush()
	if err != nil {
		log.Fatalf("Could not flush the connection: %v", err)
	}

	bm.AddPubSample(bench.NewSample(numMsg, c.msgSize, start, time.Now(), nc))

	donewg.Done()
}

func (c *benchCmd) runCorePublisher(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, trigger chan struct{}, numMsg int, offset int, pubNumber string) {
	startwg.Done()

	var progress *uiprogress.Bar

	log.Printf("Starting publisher, publishing %s messages", f(numMsg))

	if !c.noProgress {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = progressWidth()
	}

	var msg []byte
	if c.msgSize > 0 {
		msg = make([]byte, c.msgSize)
	}

	<-trigger

	// introduces some jitter between the publishers if pubSleep is set and more than one publisher
	if c.pubSleep != 0 && pubNumber != "0" {
		n := rand.Intn(int(c.pubSleep))
		time.Sleep(time.Duration(n))
	}

	start := time.Now()
	coreNATSPublisher(*c, nc, progress, msg, numMsg, offset)

	err := nc.Flush()
	if err != nil {
		log.Fatalf("Could not flush the connection: %v", err)
	}

	bm.AddPubSample(bench.NewSample(numMsg, c.msgSize, start, time.Now(), nc))

	donewg.Done()
}

func (c *benchCmd) runCoreSubscriber(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, numMsg int) {
	received := 0
	ch := make(chan time.Time, 2)
	var progress *uiprogress.Bar

	if !c.noProgress {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = progressWidth()
	}

	state := "Setup     "

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	// Core NATS Message handler
	mh := func(msg *nats.Msg) {
		received++

		if received == 1 {
			ch <- time.Now()
		}

		if received >= numMsg {
			ch <- time.Now()
		}

		if progress != nil {
			progress.Incr()
		}
	}

	state = "Receiving "

	sub, err := nc.Subscribe(getSubscribeSubject(c), mh)
	if err != nil {
		log.Fatalf("Subscribe error: %v", err)
	}

	err = sub.SetPendingLimits(-1, -1)
	if err != nil {
		log.Fatalf("Error setting pending limits on the subscriber: %v", err)
	}

	err = nc.Flush()
	if err != nil {
		log.Fatalf("Error flushing: %v", err)
	}

	startwg.Done()

	start := <-ch
	end := <-ch

	state = "Finished  "

	bm.AddSubSample(bench.NewSample(numMsg, c.msgSize, start, end, nc))

	donewg.Done()
}

func (c *benchCmd) runCoreRequester(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, trigger chan struct{}, numMsg int, offset int, pubNumber string) {
	startwg.Done()

	var progress *uiprogress.Bar

	log.Printf("Starting requester, requesting %s messages", f(numMsg))

	if !c.noProgress {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = progressWidth()
	}

	var msg []byte
	if c.msgSize > 0 {
		msg = make([]byte, c.msgSize)
	}

	<-trigger

	// introduces some jitter between the publishers if pubSleep is set and more than one publisher
	if c.pubSleep != 0 && pubNumber != "0" {
		n := rand.Intn(int(c.pubSleep))
		time.Sleep(time.Duration(n))
	}

	start := time.Now()
	coreNATSRequester(*c, nc, progress, msg, numMsg, offset)

	err := nc.Flush()
	if err != nil {
		log.Fatalf("Could not flush the connection: %v", err)
	}

	bm.AddPubSample(bench.NewSample(numMsg, c.msgSize, start, time.Now(), nc))

	donewg.Done()
}

func (c *benchCmd) runCoreReplier(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, numMsg int) {
	ch := make(chan struct{}, 1)

	log.Print("Starting replier, hit control-c to stop")

	// Core NATS Message handler
	mh := func(msg *nats.Msg) {
		time.Sleep(c.subSleep)

		err := msg.Respond([]byte("ok"))
		if err != nil {
			log.Fatalf("Error replying to the request: %v", err)
		}
	}

	sub, err := nc.QueueSubscribe(getSubscribeSubject(c), "bench-reply", mh)
	if err != nil {
		log.Fatalf("QueueSubscribe error: %v", err)
	}

	err = sub.SetPendingLimits(-1, -1)
	if err != nil {
		log.Fatalf("Error setting pending limits on the subscriber: %v", err)
	}

	err = nc.Flush()
	if err != nil {
		log.Fatalf("Error flushing: %v", err)
	}

	startwg.Done()

	<-ch

	donewg.Done()
}

func (c *benchCmd) runJSPublisher(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, trigger chan struct{}, numMsg int, offset int, idPrefix string, pubNumber string) {
	startwg.Done()

	var progress *uiprogress.Bar

	log.Printf("Starting JS publisher, publishing %s messages", f(numMsg))

	if !c.noProgress {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = progressWidth()
	}

	var msg []byte
	if c.msgSize > 0 {
		msg = make([]byte, c.msgSize)
	}

	<-trigger

	// introduces some jitter between the publishers if pubSleep is set and more than one publisher
	if c.pubSleep != 0 && pubNumber != "0" {
		n := rand.Intn(int(c.pubSleep))
		time.Sleep(time.Duration(n))
	}

	start := time.Now()
	jsPublisher(c, nc, progress, msg, numMsg, idPrefix, pubNumber, offset)

	err := nc.Flush()
	if err != nil {
		log.Fatalf("Could not flush the connection: %v", err)
	}

	bm.AddPubSample(bench.NewSample(numMsg, c.msgSize, start, time.Now(), nc))

	donewg.Done()
}

func (c *benchCmd) runJSOrdered(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, numMsg int) {
	received := 0

	ch := make(chan time.Time, 2)

	var progress *uiprogress.Bar

	log.Printf("Starting subscriber, expecting %s messages", f(numMsg))

	if !c.noProgress {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = progressWidth()
	}

	state := "Setup     "

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	// New API message handler
	mh2 := func(msg jetstream.Msg) {
		received++

		time.Sleep(c.subSleep)

		if received >= numMsg {
			ch <- time.Now()
		}

		if progress != nil {
			progress.Incr()
		}
	}

	var consumer jetstream.Consumer

	var err error

	ctx, cancel := context.WithTimeout(context.Background(), c.jsTimeout)
	defer cancel()

	js, err := getjs(nc)
	if err != nil {
		log.Fatalf("Couldn't get JetStream instance: %v", err)
	}

	// start the timer now rather than when the first message is received in JS mode
	startTime := time.Now()
	ch <- startTime
	if progress != nil {
		progress.TimeStarted = startTime
	}

	// new API
	state = "Receiving"
	s, err := js.Stream(ctx, c.streamName)
	if err != nil {
		log.Fatalf("Error getting stream %s: %v", c.streamName, err)
	}

	consumer, err = s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
	if err != nil {
		log.Fatalf("Error creating the ephemeral ordered consumer: %v", err)
	}

	cc, err := consumer.Consume(mh2, jetstream.PullMaxMessages(c.consumerBatch))
	if err != nil {
		return
	}
	defer cc.Stop()

	err = nc.Flush()
	if err != nil {
		log.Fatalf("Error flushing: %v", err)
	}

	startwg.Done()

	start := <-ch
	end := <-ch

	state = "Finished  "

	bm.AddSubSample(bench.NewSample(numMsg, c.msgSize, start, end, nc))

	donewg.Done()
}

func (c *benchCmd) runJSConsume(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, numMsg int, offset int) {
	received := 0

	ch := make(chan time.Time, 2)

	var progress *uiprogress.Bar

	log.Printf("Starting consumer, expecting %s messages", f(numMsg))

	if !c.noProgress {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = progressWidth()
	}

	state := "Setup     "

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	// New API message handler
	mh2 := func(msg jetstream.Msg) {
		received++

		time.Sleep(c.subSleep)

		if c.ack {
			err := msg.Ack()
			if err != nil {
				log.Fatalf("Error acknowledging the message: %v", err)
			}
		}

		if received >= numMsg {
			ch <- time.Now()
		}

		if progress != nil {
			progress.Incr()
		}
	}

	var consumer jetstream.Consumer

	var err error

	// create the subscriber

	ctx, cancel := context.WithTimeout(context.Background(), c.jsTimeout)
	defer cancel()

	js, err := getjs(nc)
	if err != nil {
		log.Fatalf("Couldn't get JetStream instance: %v", err)
	}

	// start the timer now rather than when the first message is received in JS mode
	startTime := time.Now()
	ch <- startTime
	if progress != nil {
		progress.TimeStarted = startTime
	}

	// new API
	state = "Consuming"
	s, err := js.Stream(ctx, c.streamName)
	if err != nil {
		log.Fatalf("Error getting stream %s: %v", c.streamName, err)
	}

	consumer, err = s.Consumer(ctx, c.consumerName)
	if err != nil {
		log.Fatalf("Error getting consumer %s: %v", c.consumerName, err)
	}

	cc, err := consumer.Consume(mh2, jetstream.PullMaxMessages(c.consumerBatch), jetstream.StopAfter(numMsg))
	if err != nil {
		return
	}
	defer cc.Stop()

	err = nc.Flush()
	if err != nil {
		log.Fatalf("Error flushing: %v", err)
	}

	startwg.Done()

	start := <-ch
	end := <-ch

	state = "Finished  "

	bm.AddSubSample(bench.NewSample(numMsg, c.msgSize, start, end, nc))

	donewg.Done()
}

func (c *benchCmd) runJSFetch(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, numMsg int, offset int) {
	received := 0

	ch := make(chan time.Time, 2)

	var progress *uiprogress.Bar

	log.Printf("Starting consumer, expecting %s messages", f(numMsg))

	if !c.noProgress {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = progressWidth()
	}

	state := "Setup     "

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	// New API message handler
	mh2 := func(msg jetstream.Msg) {
		received++
		time.Sleep(c.subSleep)

		if c.ack {
			err := msg.Ack()
			if err != nil {
				log.Fatalf("Error acknowledging the message: %v", err)
			}
		}

		if received >= numMsg {
			ch <- time.Now()
		}

		if progress != nil {
			progress.Incr()
		}
	}

	var consumer jetstream.Consumer

	var err error

	ctx, cancel := context.WithTimeout(context.Background(), c.jsTimeout)
	defer cancel()

	js, err := getjs(nc)
	if err != nil {
		log.Fatalf("Couldn't get JetStream instance: %v", err)
	}

	// start the timer now rather than when the first message is received in JS mode

	startTime := time.Now()
	ch <- startTime
	if progress != nil {
		progress.TimeStarted = startTime
	}

	// new API
	state = "Consuming"
	s, err := js.Stream(ctx, c.streamName)
	if err != nil {
		log.Fatalf("Error getting stream %s: %v", c.streamName, err)
	}

	consumer, err = s.Consumer(ctx, c.consumerName)
	if err != nil {
		log.Fatalf("Error getting consumer %s: %v", c.consumerName, err)
	}

	err = nc.Flush()
	if err != nil {
		log.Fatalf("Error flushing: %v", err)
	}

	startwg.Done()

	for i := 0; i < numMsg; {
		batchSize := func() int {
			if c.consumerBatch <= (numMsg - i) {
				return c.consumerBatch
			} else {
				return numMsg - i
			}
		}()

		if progress != nil {
			state = "Fetching  "
		}

		msgs, err := consumer.Fetch(batchSize)
		if err != nil {
			if c.noProgress {
				if err == nats.ErrTimeout {
					log.Print("Fetch  timeout!")
				} else {
					log.Printf("New consumer Fetch error: %v", err)
				}
				c.fetchTimeout = true
			}
		}
		if progress != nil {
			state = "Fetching  "
		}

		for msg := range msgs.Messages() {
			mh2(msg)
			i++
		}

		if msgs.Error() != nil {
			log.Printf("New consumer Fetch msgs error: %v", msgs.Error())
			c.fetchTimeout = true
		}

	}

	start := <-ch
	end := <-ch

	state = "Finished  "

	bm.AddSubSample(bench.NewSample(numMsg, c.msgSize, start, end, nc))

	donewg.Done()
}

func (c *benchCmd) runKVPutter(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, trigger chan struct{}, numMsg int, pubNumber string, offset int) {
	startwg.Done()

	var progress *uiprogress.Bar

	log.Printf("Starting JS publisher, publishing %s messages", f(numMsg))

	if !c.noProgress {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = progressWidth()
	}

	var msg []byte
	if c.msgSize > 0 {
		msg = make([]byte, c.msgSize)
	}

	<-trigger

	// introduces some jitter between the publishers if pubSleep is set and more than one publisher
	if c.pubSleep != 0 && pubNumber != "0" {
		n := rand.Intn(int(c.pubSleep))
		time.Sleep(time.Duration(n))
	}

	start := time.Now()
	kvPutter(*c, nc, progress, msg, numMsg, offset)

	err := nc.Flush()
	if err != nil {
		log.Fatalf("Could not flush the connection: %v", err)
	}

	bm.AddPubSample(bench.NewSample(numMsg, c.msgSize, start, time.Now(), nc))

	donewg.Done()
}

func (c *benchCmd) runKVGetter(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, numMsg int, offset int) {
	ch := make(chan time.Time, 2)

	var progress *uiprogress.Bar

	log.Printf("Starting KV getter, trying to get %s messages", f(numMsg))

	if !c.noProgress {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = progressWidth()
	}

	state := "Setup     "

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	// create the subscriber

	ctx, cancel := context.WithTimeout(context.Background(), c.jsTimeout)
	defer cancel()

	js, err := getjs(nc)
	if err != nil {
		log.Fatalf("Couldn't get JetStream instance: %v", err)
	}

	startwg.Done()

	ai, err := js.AccountInfo(ctx)
	if err != nil {
		log.Fatalf("Error getting account info: %v", err)
	}
	fmt.Printf("Account info: %+v\n", ai)

	stil := js.ListStreams(ctx)
	for si := range stil.Info() {
		fmt.Printf("Stream info: %+v\n", si)
	}

	kvBucket, err := js.KeyValue(ctx, c.bucketName)
	if err != nil {
		log.Fatalf("Couldn't find kv bucket %s: %v", c.bucketName, err)
	}

	// start the timer now rather than when the first message is received in JS mode
	startTime := time.Now()
	ch <- startTime

	if progress != nil {
		progress.TimeStarted = startTime
	}

	state = "Getting   "

	for i := 0; i < numMsg; i++ {
		entry, err := kvBucket.Get(ctx, fmt.Sprintf("%d", offset+i))

		if err != nil {
			log.Fatalf("Error getting key %d: %v", offset+i, err)
		}

		if entry.Value() == nil {
			log.Printf("Warning: got no value for key %d", offset+i)
		}

		if progress != nil {
			progress.Incr()
		}

		time.Sleep(c.subSleep)
	}

	ch <- time.Now()

	start := <-ch
	end := <-ch

	state = "Finished  "

	bm.AddSubSample(bench.NewSample(numMsg, c.msgSize, start, end, nc))

	donewg.Done()

}

func (c *benchCmd) runSubscriber(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, numMsg int, offset int) {
	received := 0

	ch := make(chan time.Time, 2)

	var progress *uiprogress.Bar

	if !c.reply {
		if c.kv {
			log.Printf("Starting KV getter, trying to get %s messages", f(numMsg))
		} else {
			log.Printf("Starting subscriber, expecting %s messages", f(numMsg))
		}
	} else {
		log.Print("Starting replier, hit control-c to stop")
		c.noProgress = true
	}

	if !c.noProgress {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = progressWidth()
	}

	state := "Setup     "

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	// Message handler
	mh := func(msg *nats.Msg) {
		received++
		if c.reply || (c.js && (c.pull || c.push)) {
			time.Sleep(c.subSleep)
			if c.ack {
				err := msg.Ack()
				if err != nil {
					log.Fatalf("Error acknowledging the  message: %v", err)
				}
			}
		}

		if !c.js && received == 1 {
			ch <- time.Now()
		}
		if received >= numMsg && !c.reply {
			ch <- time.Now()
		}
		if progress != nil {
			progress.Incr()
		}
	}

	// New API message handler
	mh2 := func(msg jetstream.Msg) {
		received++
		if c.push || c.pull {
			time.Sleep(c.subSleep)
			if c.ack {
				err := msg.Ack()
				if err != nil {
					log.Fatalf("Error acknowledging the message: %v", err)
				}
			}
		}

		if !c.js && received == 1 {
			ch <- time.Now()
		}
		if received >= numMsg && !c.reply {
			ch <- time.Now()
		}
		if progress != nil {
			progress.Incr()
		}
	}

	var sub *nats.Subscription
	var consumer jetstream.Consumer

	var err error

	if !c.kv {
		// create the subscriber
		if c.js {
			var js nats.JetStreamContext

			js, err = nc.JetStream(jsOpts()...)
			if err != nil {
				log.Fatalf("Couldn't get the JetStream context: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), c.jsTimeout)
			defer cancel()

			var js2 jetstream.JetStream

			switch {
			case opts().JsDomain != "":
				js2, err = jetstream.NewWithDomain(nc, opts().JsDomain)
			case opts().JsApiPrefix != "":
				js2, err = jetstream.NewWithAPIPrefix(nc, opts().JsApiPrefix)
			default:
				js2, err = jetstream.New(nc)
			}
			if err != nil {
				log.Fatalf("Couldn't get the new API JetStream instance: %v", err)
			}

			// start the timer now rather than when the first message is received in JS mode

			startTime := time.Now()
			ch <- startTime
			if progress != nil {
				progress.TimeStarted = startTime
			}
			if c.newJSAPI {
				// new API
				state = "Consuming"
				s, err := js2.Stream(ctx, c.streamName)
				if err != nil {
					log.Fatalf("Error getting stream %s: %v", c.streamName, err)
				}
				if c.push || c.pull {
					consumer, err = s.Consumer(ctx, c.consumerName)
					if err != nil {
						log.Fatalf("Error getting consumer %s: %v", c.consumerName, err)
					}
				} else {
					consumer, err = s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
					if err != nil {
						log.Fatalf("Error creating the ephemeral ordered consumer: %v", err)
					}
				}
				if c.push {
					cc, err := consumer.Consume(mh2, jetstream.PullMaxMessages(c.consumerBatch), jetstream.StopAfter(numMsg))
					if err != nil {
						return
					}
					defer cc.Stop()
				} else if !c.pull {
					// ordered
					cc, err := consumer.Consume(mh2, jetstream.PullMaxMessages(c.consumerBatch))
					if err != nil {
						return
					}
					defer cc.Stop()
				}
			} else if c.pull {
				sub, err = js.PullSubscribe(getSubscribeSubject(c), c.consumerName, nats.BindStream(c.streamName))
				if err != nil {
					log.Fatalf("Error PullSubscribe: %v", err)
				}
				defer sub.Drain()
			} else if c.push {
				state = "Receiving "
				sub, err = js.QueueSubscribe(getSubscribeSubject(c), c.consumerName+"-GROUP", mh, nats.Bind(c.streamName, c.consumerName), nats.ManualAck())
				if err != nil {
					log.Fatalf("Error push durable Subscribe: %v", err)
				}
				_ = sub.AutoUnsubscribe(numMsg)

			} else {
				state = "Consuming "
				// ordered push consumer
				sub, err = js.Subscribe(getSubscribeSubject(c), mh, nats.OrderedConsumer())
				if err != nil {
					log.Fatalf("Push consumer Subscribe error: %v", err)
				}
			}
		} else {
			state = "Receiving "
			if !c.reply {
				sub, err = nc.Subscribe(getSubscribeSubject(c), mh)
				if err != nil {
					log.Fatalf("Subscribe error: %v", err)
				}
			} else {
				sub, err = nc.QueueSubscribe(getSubscribeSubject(c), "bench-reply", mh)
				if err != nil {
					log.Fatalf("QueueSubscribe error: %v", err)
				}
			}
		}

		if !c.newJSAPI {
			err = sub.SetPendingLimits(-1, -1)
			if err != nil {
				log.Fatalf("Error setting pending limits on the subscriber: %v", err)
			}
		}
	}

	err = nc.Flush()
	if err != nil {
		log.Fatalf("Error flushing: %v", err)
	}

	startwg.Done()

	if c.kv {
		if c.newJSAPI {
			ctx, cancel := context.WithTimeout(context.Background(), c.jsTimeout)
			defer cancel()

			var js2 jetstream.JetStream

			switch {
			case opts().JsDomain != "":
				js2, err = jetstream.NewWithDomain(nc, opts().JsDomain)
			case opts().JsApiPrefix != "":
				js2, err = jetstream.NewWithAPIPrefix(nc, opts().JsApiPrefix)
			default:
				js2, err = jetstream.New(nc)
			}
			if err != nil {
				log.Fatalf("Couldn't get the new API JetStream instance: %v", err)
			}

			kvBucket, err := js2.KeyValue(ctx, c.bucketName)
			if err != nil {
				log.Fatalf("Couldn't find kv bucket %s: %v", c.bucketName, err)
			}

			// start the timer now rather than when the first message is received in JS mode
			startTime := time.Now()
			ch <- startTime

			if progress != nil {
				progress.TimeStarted = startTime
			}

			state = "Getting   "

			for i := 0; i < numMsg; i++ {
				entry, err := kvBucket.Get(ctx, fmt.Sprintf("%d", offset+i))

				if err != nil {
					log.Fatalf("Error getting key %d: %v", offset+i, err)
				}

				if entry.Value() == nil {
					log.Printf("Warning: got no value for key %d", offset+i)
				}

				if progress != nil {
					progress.Incr()
				}

				time.Sleep(c.subSleep)
			}

			ch <- time.Now()
		} else {
			var js nats.JetStreamContext

			js, err = nc.JetStream(jsOpts()...)
			if err != nil {
				log.Fatalf("Couldn't get the JetStream context: %v", err)
			}

			kvBucket, err := js.KeyValue(c.bucketName)
			if err != nil {
				log.Fatalf("Couldn't find kv bucket %s: %v", c.bucketName, err)
			}

			// start the timer now rather than when the first message is received in JS mode
			startTime := time.Now()
			ch <- startTime

			if progress != nil {
				progress.TimeStarted = startTime
			}

			state = "Getting   "

			for i := 0; i < numMsg; i++ {
				entry, err := kvBucket.Get(fmt.Sprintf("%d", offset+i))
				if err != nil {
					log.Fatalf("Error getting key %d: %v", offset+i, err)
				}

				if entry.Value() == nil {
					log.Printf("Warning: got no value for key %d", offset+i)
				}

				if progress != nil {
					progress.Incr()
				}

				time.Sleep(c.subSleep)
			}

			ch <- time.Now()
		}
	} else if c.js && c.pull {
		for i := 0; i < numMsg; {
			batchSize := func() int {
				if c.consumerBatch <= (numMsg - i) {
					return c.consumerBatch
				} else {
					return numMsg - i
				}
			}()

			if progress != nil {
				if c.pull {
					state = "Pulling   "
				} else if c.newJSAPI {
					state = "Consuming "
				}
			}

			if c.newJSAPI {
				if c.pull {
					consumer.Messages()
				}
				msgs, err := consumer.Fetch(batchSize)
				if err != nil {
					if c.noProgress {
						if err == nats.ErrTimeout {
							log.Print("Fetch  timeout!")
						} else {
							log.Printf("New consumer Fetch error: %v", err)
						}
						c.fetchTimeout = true
					}

					if progress != nil {
						state = "Fetching  "
					}

					for msg := range msgs.Messages() {
						mh2(msg)
						i++
					}

					if msgs.Error() != nil {
						log.Printf("New consumer Fetch msgs error: %v", msgs.Error())
						c.fetchTimeout = true
					}
				}
			} else if c.pull {
				msgs, err := sub.Fetch(batchSize, nats.MaxWait(c.jsTimeout))
				if err == nil {
					if progress != nil {
						state = "Handling  "
					}

					for _, msg := range msgs {
						mh(msg)
						i++
					}
				} else {
					if c.noProgress {
						if err == nats.ErrTimeout {
							log.Print("Fetch timeout!")
						} else {
							log.Printf("Pull consumer Fetch error: %v", err)
						}
					}
					c.fetchTimeout = true
				}
			}
		}
	}

	start := <-ch
	end := <-ch

	state = "Finished  "

	bm.AddSubSample(bench.NewSample(numMsg, c.msgSize, start, end, nc))

	donewg.Done()
}
