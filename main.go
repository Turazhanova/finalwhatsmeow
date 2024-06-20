package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/skip2/go-qrcode"
	"io/ioutil"
	"mime"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mdp/qrterminal/v3"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	waBinary "go.mau.fi/whatsmeow/binary"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

var (
	client          *whatsmeow.Client
	log             waLog.Logger
	csvMutex        sync.Mutex
	logLevel        = "INFO"
	debugLogs       = flag.Bool("debug", false, "Enable debug logs?")
	dbDialect       = flag.String("db-dialect", "sqlite3", "Database dialect (sqlite3 or postgres)")
	dbAddress       = flag.String("db-address", "file:/data/mdtest.db?_foreign_keys=on", "Database address")
	requestFullSync = flag.Bool("request-full-sync", false, "Request full (1 year) history sync when logging in?")
	pairRejectChan  = make(chan bool, 1)
	historySyncID   int32
	startupTime     = time.Now().Unix()
	programID       = flag.String("program-id", "", "Unique identifier for the program") // Initialize as an empty string
)

type HostInfo struct {
	Role  string `json:"role"`
	Phone string `json:"phone"`
	Host  string `json:"host"`
	Port  string `json:"port"`
}

var hostData = []HostInfo{
	{Role: "admin", Phone: "77007727858", Host: "localhost", Port: "8081"},
	{Role: "user", Phone: "77009809778", Host: "localhost", Port: "8082"},
	// Добавьте здесь другие записи по необходимости
}

func getHostInfoHandler(w http.ResponseWriter, r *http.Request) {
	role := r.URL.Query().Get("role")
	phone := r.URL.Query().Get("phone")

	if role == "" || phone == "" {
		http.Error(w, "Role and phone parameters are required", http.StatusBadRequest)
		return
	}

	for _, hostInfo := range hostData {
		if hostInfo.Role == role && hostInfo.Phone == phone {
			response, err := json.Marshal(hostInfo)
			if err != nil {
				http.Error(w, "Failed to generate JSON", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(response)
			return
		}
	}

	http.Error(w, "Host info not found", http.StatusNotFound)
}

func main() {
	waBinary.IndentXML = true

	// Check if PROGRAM_ID environment variable is set and use it as the default value for program-id flag
	if envProgramID := os.Getenv("PROGRAM_ID"); envProgramID != "" {
		flag.Set("program-id", envProgramID)
	}

	flag.Parse()

	if *programID == "" {
		fmt.Println("Program ID is required")
		return
	}

	if *debugLogs {
		logLevel = "DEBUG"
	}
	if *requestFullSync {
		store.DeviceProps.RequireFullSync = proto.Bool(true)
		store.DeviceProps.HistorySyncConfig = &waProto.DeviceProps_HistorySyncConfig{
			FullSyncDaysLimit:   proto.Uint32(3650),
			FullSyncSizeMbLimit: proto.Uint32(102400),
			StorageQuotaMb:      proto.Uint32(102400),
		}
	}
	log = waLog.Stdout("Main", logLevel, true)

	dbLog := waLog.Stdout("Database", logLevel, true)
	storeContainer, err := sqlstore.New(*dbDialect, *dbAddress, dbLog)
	if err != nil {
		log.Errorf("Failed to connect to database: %v", err)
		return
	}

	if err := initDB(); err != nil {
		log.Errorf("Failed to initialize database: %v", err)
		return
	}

	device, err := storeContainer.GetFirstDevice()
	if err != nil {
		log.Errorf("Failed to get device: %v", err)
		return
	}

	client = whatsmeow.NewClient(device, waLog.Stdout("Client", logLevel, true))
	if client == nil {
		log.Errorf("WhatsApp client is not initialized")
		return
	}

	var isWaitingForPair atomic.Bool
	client.PrePairCallback = func(jid types.JID, platform, businessName string) bool {
		isWaitingForPair.Store(true)
		defer isWaitingForPair.Store(false)
		log.Infof("Pairing %s (platform: %q, business name: %q). Type r within 3 seconds to reject pair", jid, platform, businessName)
		select {
		case reject := <-pairRejectChan:
			if reject {
				log.Infof("Rejecting pair")
				return false
			}
		case <-time.After(3 * time.Second):
		}
		log.Infof("Accepting pair")
		return true
	}

	// Ensure the QR channel is set up before connecting
	if client.Store.ID == nil {
		log.Infof("Attempting to get QR channel")
		ch, err := client.GetQRChannel(context.Background())
		if err != nil {
			log.Errorf("Failed to get QR channel: %v", err)
			return
		}
		go func() {
			for evt := range ch {
				if evt.Event == "code" {
					qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)

					// Save QR code as text in a .txt file
					txtFile, err := os.Create("qr_code.txt")
					if err != nil {
						log.Errorf("Failed to create text file: %v", err)
						return
					}
					defer txtFile.Close()
					_, err = txtFile.WriteString(evt.Code)
					if err != nil {
						log.Errorf("Failed to write to text file: %v", err)
						return
					}

					// Save QR code as an image in a .png file
					err = qrcode.WriteFile(evt.Code, qrcode.Medium, 256, "qr_code.png")
					if err != nil {
						log.Errorf("Failed to save QR code image: %v", err)
						return
					}

					log.Infof("QR code saved as text and image")
				} else {
					log.Infof("QR channel result: %s", evt.Event)
				}
			}
		}()
	} else {
		log.Infof("Client is already logged in, no need to get QR code")
	}

	log.Infof("Attempting to connect the client")
	err = client.Connect()
	if err != nil {
		log.Errorf("Failed to connect: %v", err)
		return
	}

	client.AddEventHandler(receiveHandler)

	// Start HTTP server
	router := http.NewServeMux()
	router.HandleFunc("/send", sendMessageHandler)
	router.HandleFunc("/qr-text", qrTextHandler)
	router.HandleFunc("/qr-photo", qrPhotoHandler)
	router.HandleFunc("/get-host-info", getHostInfoHandler)       // Новый маршрут
	router.HandleFunc("/get-all-messages", getAllMessagesHandler) // Новый маршрут для получения всех сообщений
	router.HandleFunc("/check-user", checkUserHandler)            // Новый маршрут для проверки номера телефона

	go http.ListenAndServe(":8080", router)

	if os.Getenv("DOCKER_MODE") == "true" {
		select {} // Prevent the application from exiting in Docker mode
	} else {
		c := make(chan os.Signal, 1)
		input := make(chan string)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		go func() {
			defer close(input)
			scan := bufio.NewScanner(os.Stdin)
			for scan.Scan() {
				line := strings.TrimSpace(scan.Text())
				if len(line) > 0 {
					input <- line
				}
			}
		}()
		for {
			select {
			case <-c:
				log.Infof("Interrupt received, exiting")
				client.Disconnect()
				return
			case cmd := <-input:
				if len(cmd) == 0 {
					log.Infof("Stdin closed, exiting")
					client.Disconnect()
					return
				}
				if isWaitingForPair.Load() {
					if cmd == "r" {
						pairRejectChan <- true
					} else if cmd == "a" {
						pairRejectChan <- false
					}
					continue
				}
				args := strings.Fields(cmd)
				cmd = args[0]
				args = args[1:]
				go handleCmd(strings.ToLower(cmd), args)
			}
		}
	}
}

func writeMessageToDB(programID, messageID, phone, msgType, text, dateTime string) error {
	db, err := sql.Open(*dbDialect, *dbAddress)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}
	defer db.Close()

	query := `INSERT INTO messages (program_id, message_id, phone, message_type, text, date_time) VALUES (?, ?, ?, ?, ?, ?)`
	_, err = db.Exec(query, programID, messageID, phone, msgType, text, dateTime)
	if err != nil {
		log.Errorf("Failed to insert message into database: %v", err)
		return fmt.Errorf("failed to insert message into database: %v", err)
	}
	log.Infof("Message inserted into database: %s", messageID)
	return nil
}

func parseJID(arg string) (types.JID, bool) {
	if arg[0] == '+' {
		arg = arg[1:]
	}
	if !strings.ContainsRune(arg, '@') {
		return types.NewJID(arg, types.DefaultUserServer), true
	} else {
		recipient, err := types.ParseJID(arg)
		if err != nil {
			log.Errorf("Invalid JID %s: %v", arg, err)
			return recipient, false
		} else if recipient.User == "" {
			log.Errorf("Invalid JID %s: no server specified", arg)
			return recipient, false
		}
		return recipient, true
	}
}
func handleCmd(cmd string, args []string) {
	switch cmd {
	case "pair-phone":
		if len(args) < 1 {
			log.Errorf("Usage: pair-phone <number>")
			return
		}
		linkingCode, err := client.PairPhone(args[0], true, whatsmeow.PairClientChrome, "Chrome (Linux)")
		if err != nil {
			panic(err)
		}
		fmt.Println("Linking code:", linkingCode)
	case "reconnect":
		client.Disconnect()
		err := client.Connect()
		if err != nil {
			log.Errorf("Failed to connect: %v", err)
		}
	case "logout":
		err := client.Logout()
		if err != nil {
			log.Errorf("Error logging out: %v", err)
		} else {
			log.Infof("Successfully logged out")
		}
	case "appstate":
		if len(args) < 1 {
			log.Errorf("Usage: appstate <types...>")
			return
		}
		names := []appstate.WAPatchName{appstate.WAPatchName(args[0])}
		if args[0] == "all" {
			names = []appstate.WAPatchName{appstate.WAPatchRegular, appstate.WAPatchRegularHigh, appstate.WAPatchRegularLow, appstate.WAPatchCriticalUnblockLow, appstate.WAPatchCriticalBlock}
		}
		resync := len(args) > 1 && args[1] == "resync"
		for _, name := range names {
			err := client.FetchAppState(name, resync, false)
			if err != nil {
				log.Errorf("Failed to sync app state: %v", err)
			}
		}
	case "request-appstate-key":
		if len(args) < 1 {
			log.Errorf("Usage: request-appstate-key <ids...>")
			return
		}
		var keyIDs = make([][]byte, len(args))
		for i, id := range args {
			decoded, err := hex.DecodeString(id)
			if err != nil {
				log.Errorf("Failed to decode %s as hex: %v", id, err)
				return
			}
			keyIDs[i] = decoded
		}
		client.DangerousInternals().RequestAppStateKeys(context.Background(), keyIDs)
	case "unavailable-request":
		if len(args) < 3 {
			log.Errorf("Usage: unavailable-request <chat JID> <sender JID> <message ID>")
			return
		}
		chat, ok := parseJID(args[0])
		if !ok {
			return
		}
		sender, ok := parseJID(args[1])
		if !ok {
			return
		}
		resp, err := client.SendMessage(
			context.Background(),
			client.Store.ID.ToNonAD(),
			client.BuildUnavailableMessageRequest(chat, sender, args[2]),
			whatsmeow.SendRequestExtra{Peer: true},
		)
		fmt.Println(resp)
		fmt.Println(err)
	case "checkuser":
		if len(args) < 1 {
			log.Errorf("Usage: checkuser <phone numbers...>")
			return
		}
		resp, err := client.IsOnWhatsApp(args)
		if err != nil {
			log.Errorf("Failed to check if users are on WhatsApp:", err)
		} else {
			for _, item := range resp {
				if item.VerifiedName != nil {
					log.Infof("%s: on whatsapp: %t, JID: %s, business name: %s", item.Query, item.IsIn, item.JID, item.VerifiedName.Details.GetVerifiedName())
				} else {
					log.Infof("%s: on whatsapp: %t, JID: %s", item.Query, item.IsIn, item.JID)
				}
			}
		}

	}
}

func receiveHandler(rawEvt interface{}) {
	switch event := rawEvt.(type) {
	case *events.Message:
		metaParts := []string{
			fmt.Sprintf("pushname: %s", event.Info.PushName),
			fmt.Sprintf("timestamp: %s", event.Info.Timestamp.String()),
		}
		if event.Info.Type != "" {
			metaParts = append(metaParts, fmt.Sprintf("type: %s", event.Info.Type))
		}
		if event.Info.Category != "" {
			metaParts = append(metaParts, fmt.Sprintf("category: %s", event.Info.Category))
		}
		if event.IsViewOnce {
			metaParts = append(metaParts, "view once")
		}
		if event.IsEphemeral {
			metaParts = append(metaParts, "ephemeral")
		}
		if event.IsViewOnceV2 {
			metaParts = append(metaParts, "ephemeral (v2)")
		}
		if event.IsDocumentWithCaption {
			metaParts = append(metaParts, "document with caption")
		}
		if event.IsEdit {
			metaParts = append(metaParts, "edit")
		}

		log.Infof("Received message %s from %s (%s): %+v", event.Info.ID, event.Info.SourceString(), strings.Join(metaParts, ", "), event.Message)

		var filePath, msgType string

		if img := event.Message.GetImageMessage(); img != nil {
			data, err := client.Download(img)
			if err != nil {
				log.Errorf("Failed to download image: %v", err)
				return
			}
			exts, _ := mime.ExtensionsByType(img.GetMimetype())
			if len(exts) == 0 {
				log.Warnf("Failed to determine file extension for mimetype: %s, using fallback .jpg", img.GetMimetype())
				exts = append(exts, ".jpg") // Fallback extension for images
			}
			log.Infof("Image file extension determined: %s", exts[0])
			filePath = fmt.Sprintf("media/image/%s%s", event.Info.ID, exts[0])
			err = saveMediaFile(filePath, data)
			if err != nil {
				log.Errorf("Failed to save image: %v", err)
				return
			}
			log.Infof("Saved image in message to %s", filePath)
			msgType = "image"
		} else if vid := event.Message.GetVideoMessage(); vid != nil {
			data, err := client.Download(vid)
			if err != nil {
				log.Errorf("Failed to download video: %v", err)
				return
			}
			exts, _ := mime.ExtensionsByType(vid.GetMimetype())
			if len(exts) == 0 {
				log.Warnf("Failed to determine file extension for mimetype: %s, using fallback .mp4", vid.GetMimetype())
				exts = append(exts, ".mp4") // Fallback extension for videos
			}
			log.Infof("Video file extension determined: %s", exts[0])
			filePath = fmt.Sprintf("media/video/%s%s", event.Info.ID, exts[0])
			err = saveMediaFile(filePath, data)
			if err != nil {
				log.Errorf("Failed to save video: %v", err)
				return
			}
			log.Infof("Saved video in message to %s", filePath)
			msgType = "video"
		} else if aud := event.Message.GetAudioMessage(); aud != nil {
			data, err := client.Download(aud)
			if err != nil {
				log.Errorf("Failed to download audio: %v", err)
				return
			}
			filePath = fmt.Sprintf("media/audio/%s.ogg", event.Info.ID)
			err = saveMediaFile(filePath, data)
			if err != nil {
				log.Errorf("Failed to save audio: %v", err)
				return
			}
			log.Infof("Saved audio in message to %s", filePath)
			msgType = "audio"
		} else if doc := event.Message.GetDocumentMessage(); doc != nil {
			data, err := client.Download(doc)
			if err != nil {
				log.Errorf("Failed to download document: %v", err)
				return
			}
			exts, _ := mime.ExtensionsByType(doc.GetMimetype())
			if len(exts) == 0 {
				log.Warnf("Failed to determine file extension for mimetype: %s, using fallback .pdf", doc.GetMimetype())
				exts = append(exts, ".pdf") // Fallback extension for documents
			}
			log.Infof("Document file extension determined: %s", exts[0])
			filePath = fmt.Sprintf("media/document/%s%s", event.Info.ID, exts[0])
			err = saveMediaFile(filePath, data)
			if err != nil {
				log.Errorf("Failed to save document: %v", err)
				return
			}
			log.Infof("Saved document in message to %s", filePath)
			msgType = "document"
		} else {
			msgType = event.Info.Type
		}

		var text string
		if filePath != "" {
			text = filePath
		} else {
			text = event.Message.GetConversation()
		}

		err := writeMessageToCSV(*programID, event.Info.ID, event.Info.SourceString(), msgType, text, event.Info.Timestamp.String())
		if err != nil {
			log.Errorf("Failed to write message to CSV: %v", err)
		}

		err = writeMessageToDB(*programID, event.Info.ID, event.Info.SourceString(), msgType, text, event.Info.Timestamp.String())
		if err != nil {
			log.Errorf("Failed to write message to database: %v", err)
		}
	}
}

func qrTextHandler(w http.ResponseWriter, r *http.Request) {
	qrContent, err := ioutil.ReadFile("qr_code.txt")
	if err != nil {
		http.Error(w, "Failed to read QR code file", http.StatusInternalServerError)
		return
	}
	qrData := map[string]string{
		"qr_code": string(qrContent),
	}
	jsonData, err := json.Marshal(qrData)
	if err != nil {
		http.Error(w, "Failed to generate JSON", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonData)
}

func qrPhotoHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "image/png")
	http.ServeFile(w, r, "qr_code.png")
}

func sendMessageHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		JID       string `json:"jid"`
		Text      string `json:"text,omitempty"`
		ImagePath string `json:"image_path,omitempty"`
		VideoPath string `json:"video_path,omitempty"`
		AudioPath string `json:"audio_path,omitempty"`
		Buttons   []struct {
			DisplayText string `json:"display_text"`
			ButtonID    string `json:"button_id"`
		} `json:"buttons,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	jid, ok := parseJID(req.JID)
	if !ok {
		http.Error(w, "Invalid JID", http.StatusBadRequest)
		return
	}

	var message *waProto.Message
	switch {
	case req.ImagePath != "":
		data, err := os.ReadFile(req.ImagePath)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to read image file: %v", err), http.StatusInternalServerError)
			return
		}
		uploaded, err := client.Upload(context.Background(), data, whatsmeow.MediaImage)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to upload image: %v", err), http.StatusInternalServerError)
			return
		}
		message = &waProto.Message{
			ImageMessage: &waProto.ImageMessage{
				Caption:       proto.String(req.Text),
				URL:           proto.String(uploaded.URL),
				DirectPath:    proto.String(uploaded.DirectPath),
				MediaKey:      uploaded.MediaKey,
				Mimetype:      proto.String(http.DetectContentType(data)),
				FileEncSHA256: uploaded.FileEncSHA256,
				FileSHA256:    uploaded.FileSHA256,
				FileLength:    proto.Uint64(uint64(len(data))),
			},
		}
	case req.VideoPath != "":
		data, err := os.ReadFile(req.VideoPath)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to read video file: %v", err), http.StatusInternalServerError)
			return
		}
		uploaded, err := client.Upload(context.Background(), data, whatsmeow.MediaVideo)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to upload video: %v", err), http.StatusInternalServerError)
			return
		}
		message = &waProto.Message{
			VideoMessage: &waProto.VideoMessage{
				Caption:       proto.String(req.Text),
				URL:           proto.String(uploaded.URL),
				DirectPath:    proto.String(uploaded.DirectPath),
				MediaKey:      uploaded.MediaKey,
				Mimetype:      proto.String(http.DetectContentType(data)),
				FileEncSHA256: uploaded.FileEncSHA256,
				FileSHA256:    uploaded.FileSHA256,
				FileLength:    proto.Uint64(uint64(len(data))),
			},
		}
	case req.AudioPath != "":
		data, err := os.ReadFile(req.AudioPath)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to read audio file: %v", err), http.StatusInternalServerError)
			log.Errorf("Failed to read audio file: %v", err)
			return
		}
		// mp3 => ogg

		log.Infof("Uploading audio file: %s", req.AudioPath)
		uploaded, err := client.Upload(context.Background(), data, whatsmeow.MediaAudio)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to upload audio: %v", err), http.StatusInternalServerError)
			log.Errorf("Failed to upload audio: %v", err)
			return
		}
		log.Infof("Audio uploaded successfully: URL=%s", uploaded.URL)
		message = &waProto.Message{
			AudioMessage: &waProto.AudioMessage{
				URL:           proto.String(uploaded.URL),
				DirectPath:    proto.String(uploaded.DirectPath),
				MediaKey:      uploaded.MediaKey,
				Mimetype:      proto.String("audio/ogg; codecs=opus"),
				FileEncSHA256: uploaded.FileEncSHA256,
				FileSHA256:    uploaded.FileSHA256,
				FileLength:    proto.Uint64(uint64(len(data))),
			},
		}
	case len(req.Buttons) > 0:
		buttons := make([]*waProto.ButtonsMessage_Button, len(req.Buttons))
		for i, btn := range req.Buttons {
			buttons[i] = &waProto.ButtonsMessage_Button{
				ButtonID: proto.String(btn.ButtonID),
				ButtonText: &waProto.ButtonsMessage_Button_ButtonText{
					DisplayText: proto.String(btn.DisplayText),
				},
				Type: waProto.ButtonsMessage_Button_RESPONSE.Enum(),
			}
		}
		message = &waProto.Message{
			ButtonsMessage: &waProto.ButtonsMessage{
				ContentText: proto.String(req.Text),
				Buttons:     buttons,
				FooterText:  proto.String(""),
				HeaderType:  waProto.ButtonsMessage_TEXT.Enum(),
			},
		}
	default:
		message = &waProto.Message{
			Conversation: proto.String(req.Text),
		}
	}

	resp, err := client.SendMessage(context.Background(), jid, message)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to send message: %v", err), http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	}
}

func writeMessageToCSV(programID, id, phone, msgType, text, dateTime string) error {
	csvMutex.Lock()
	defer csvMutex.Unlock()

	file, err := os.OpenFile("messages.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	record := []string{programID, id, phone, msgType, text, dateTime}
	return writer.Write(record)
}
func saveMediaFile(filePath string, data []byte) error {
	return os.WriteFile(filePath, data, 0600)
}
func getAllMessagesHandler(w http.ResponseWriter, r *http.Request) {
	db, err := sql.Open(*dbDialect, *dbAddress)
	if err != nil {
		http.Error(w, "Failed to connect to database", http.StatusInternalServerError)
		log.Errorf("Failed to connect to database: %v", err)
		return
	}
	defer db.Close()

	rows, err := db.Query("SELECT program_id, message_id, phone, message_type, text, date_time FROM messages")
	if err != nil {
		http.Error(w, "Failed to query messages", http.StatusInternalServerError)
		log.Errorf("Failed to query messages: %v", err)
		return
	}
	defer rows.Close()

	var messages []map[string]string
	for rows.Next() {
		var programID, messageID, phone, msgType, text, dateTime string
		if err := rows.Scan(&programID, &messageID, &phone, &msgType, &text, &dateTime); err != nil {
			http.Error(w, "Failed to scan message", http.StatusInternalServerError)
			log.Errorf("Failed to scan message: %v", err)
			return
		}
		messages = append(messages, map[string]string{
			"program_id":   programID,
			"message_id":   messageID,
			"phone":        phone,
			"message_type": msgType,
			"text":         text,
			"date_time":    dateTime,
		})
	}

	if err = rows.Err(); err != nil {
		http.Error(w, "Error iterating over rows", http.StatusInternalServerError)
		log.Errorf("Error iterating over rows: %v", err)
		return
	}

	jsonData, err := json.Marshal(messages)
	if err != nil {
		http.Error(w, "Failed to generate JSON", http.StatusInternalServerError)
		log.Errorf("Failed to generate JSON: %v", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonData)
}
func initDB() error {
	db, err := sql.Open(*dbDialect, *dbAddress)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}
	defer db.Close()

	query := `CREATE TABLE IF NOT EXISTS messages (
        program_id TEXT,
        message_id TEXT,
        phone TEXT,
        message_type TEXT,
        text TEXT,
        date_time TEXT
    );`
	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create messages table: %v", err)
	}
	return nil
}
func checkUserHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PhoneNumber string `json:"phone_number"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}
	if req.PhoneNumber == "" {
		http.Error(w, "Phone number is required", http.StatusBadRequest)
		return
	}

	resp, err := client.IsOnWhatsApp([]string{req.PhoneNumber})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to check phone number: %v", err), http.StatusInternalServerError)
		return
	}

	if len(resp) > 0 && resp[0].IsIn {
		json.NewEncoder(w).Encode(map[string]bool{"exists": true})
	} else {
		json.NewEncoder(w).Encode(map[string]bool{"exists": false})
	}
}
