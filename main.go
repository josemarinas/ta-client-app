package main
import(
	log "github.com/sirupsen/logrus"
	"fmt"
	"bufio"
	"os"
	"strings"
	"time"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	. "github.com/logrusorgru/aurora"
	"github.com/rs/xid"    
	"github.com/AlecAivazis/survey/v2"
)
var basicQs = []*survey.Question{
	{
		Name:     "username",
		Prompt:   &survey.Input{Message: "Username:"},
	},
	{
		Name: "function",
		Prompt: &survey.Select{
				Message: "Choose a function:",
				Options: []string{"Echo", "Search", "Download"},
				Default: "Echo",
		},
	},
}
const sqsMaxMessages int64 = 1
const sqsPollWaitSeconds int64 = 1
var sess = session.Must(session.NewSessionWithOptions(session.Options{
	SharedConfigState: session.SharedConfigEnable,
}))
var sqsService = sqs.New(sess)
var s3Service = s3.New(sess)
var downloader = s3manager.NewDownloader(sess)
var token = xid.New().String()
var bucket = "ta-bucket-josemarinas"
func main() {
	basicInfo := struct {
		Username	string
		Function 	string
	}{}
	inputQueue, err := getQueueUrlByTag("Flow", "input")
	if err != nil {
		log.Errorf("Error getting input queue")
		return
	}
	outputQueue, err := getQueueUrlByTag("Flow", "output")
	if err != nil {
		log.Errorf("Error getting output queue")
		return
	}
	reader := bufio.NewReader(os.Stdin)
	err = survey.Ask(basicQs,  &basicInfo)
	if (basicInfo.Function == "Download") {
		downloadAllConversation(&basicInfo.Username)
	} else {
		fmt.Printf("Enter message: ")
		outMsgChan := make(chan string, 1)
		inMsgChan := make(chan *sqs.Message, sqsMaxMessages)
		go func(){
			for {
				message, _ := reader.ReadString('\n')
				message = strings.TrimSuffix(message, "\n")
				message = strings.TrimSpace(message)
				outMsgChan <- message	
			}
		}()
		go onInput(outMsgChan, &inputQueue, &basicInfo.Username, &basicInfo.Function)
		go pollQueue(inMsgChan, &basicInfo.Username, &outputQueue)
		for message := range inMsgChan {
			if (*message.MessageAttributes["Command"].StringValue == "echo") {
				fmt.Printf("[%s]\nReceived message: %s\nEnter message: ",Blue(time.Now().Format(time.RFC1123)), Yellow(*message.Body))
				deleteMessage(message.ReceiptHandle, &outputQueue)
			} else {
				fmt.Printf("[%s]\nSearch Results:\n%s\nEnter message: ", Blue(time.Now().Format(time.RFC1123)), Yellow(*message.Body))
				deleteMessage(message.ReceiptHandle, &outputQueue)
			}
		}
	}
}
func onInput(chn chan string, queue *string, user *string, function *string)(){
	for {
		msg := <-chn
		switch *function {
		case "Search":
			sendMessage(&msg, queue, user, "search")
		case "Echo":
			if ( msg == "END") {
				os.Exit(0)
			} else {
				sendMessage(&msg, queue, user, "echo")
			}
		default:
			sendMessage(&msg , queue, user, "echo")
		}
	}
} 
func sendMessage(message *string, queue *string, user *string, command string) {
		_, err := sqsService.SendMessage(&sqs.SendMessageInput{
			QueueUrl:            	queue,
			MessageBody:					message,
			// MessageGroupId:				aws.String(token),
			// MessageDeduplicationId: aws.String(xid.New().String()),
			MessageAttributes: map[string]*sqs.MessageAttributeValue{
				"User": &sqs.MessageAttributeValue{
						DataType:    aws.String("String"),
						StringValue: user,
				},
				"Command": &sqs.MessageAttributeValue{
						DataType:    aws.String("String"),
						StringValue: aws.String(command),
				},
				"Session": &sqs.MessageAttributeValue{
						DataType:    aws.String("String"),
						StringValue: aws.String(token),
				},
			},
		})
		if err != nil {
      log.Errorf("Failed to send sqs message %v", err)
		}
		fmt.Printf("[%s]\nSent message: %s\n",Blue(time.Now().Format(time.RFC1123)), Green(*message))
}

func deleteMessage(receiptHandle *string, queue *string) {
	_, err := sqsService.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:            	queue,
		ReceiptHandle:				receiptHandle,
	})
	if err != nil {
		log.Errorf("Failed to delete sqs message %v", err)
	}
}

func pollQueue(chn chan<- *sqs.Message, user *string, queue *string) {
  for {
    output, err := sqsService.ReceiveMessage(&sqs.ReceiveMessageInput{
			AttributeNames:					aws.StringSlice([]string{"SentTimestamp"}),
      QueueUrl:            		queue,
      MaxNumberOfMessages: 		aws.Int64(sqsMaxMessages),
			WaitTimeSeconds:     		aws.Int64(sqsPollWaitSeconds),
			MessageAttributeNames:	aws.StringSlice([]string{"User", "Command", "Session"}),
    })
    if err != nil {
      log.Errorf("Failed to fetch sqs message %v", err)
    }
    for _, message := range output.Messages {
			if (
				*message.MessageAttributes["User"].StringValue == *user &&
				(*message.MessageAttributes["Command"].StringValue == "echo" ||
				*message.MessageAttributes["Command"].StringValue == "search") &&
				*message.MessageAttributes["Session"].StringValue == token ){
				chn <- message
			} else {
				sqsService.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
					QueueUrl:	queue,
					ReceiptHandle: message.ReceiptHandle,
					VisibilityTimeout: aws.Int64(0),
				})
				log.Warnf("Client App cant handle this request")
			}
    }
  }
}

func downloadAllConversation(user *string) {
	params := &s3.ListObjectsInput {
    Bucket: aws.String(bucket),
    Prefix: aws.String(fmt.Sprintf("%s", *user)),
	}
	resp, _ := s3Service.ListObjects(params)
	var response string
	for _, object := range resp.Contents {
		buff := &aws.WriteAtBuffer{}
		_, err := downloader.Download(buff, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    object.Key,
		})
		if err != nil {
			log.Errorf("Error downloading S3 object: %v", err)
		}
		response = fmt.Sprintf("%s\nSESSION: %s\n%s", response, token, string(buff.Bytes()))
	}
	f, err := os.Create(fmt.Sprintf("%s.txt", *user))
	if err != nil {
			fmt.Printf("Error creating file: %s", err)
	}
	_, err = f.WriteString(response)
	if err != nil {
		fmt.Printf("Error writing to file: %s", err)
	}
	log.Infof("Downloaded conversation succesfully, saved in file %s.txt", *user)
	defer f.Close()
	os.Exit(0)
}

func getQueueUrlByTag(tag string, tagValue string)(string, error) {
	result, err := sqsService.ListQueues(nil)
	if err != nil {
		fmt.Println("Error", err)
		return "", err
	}
	for _, url := range result.QueueUrls {
		if url == nil {
		  continue
		}
		queue := &sqs.ListQueueTagsInput{
    	QueueUrl: url,
		}
		tags, err := sqsService.ListQueueTags(queue)
		if url == nil {
		  return "", err
		}
		// fmt.Println(tags)
		if (*tags.Tags[tag] == tagValue) {
			return *url, nil
		}
	}
	return "", fmt.Errorf("Cant find queue with tag `%s = %s`", tag, tagValue)
}
