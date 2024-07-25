package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	outputPlugin = "pgoutput"
	msgTimeout   = 10 * time.Second
)

func main() {
	ctx := context.Background()

	conn, err := pgconn.Connect(ctx, os.Getenv("PGLOGREPL_DEMO_CONN_STRING"))
	if err != nil {
		log.Fatalln("failed to connect to PostgreSQL server:", err)
	}

	defer conn.Close(ctx)

	result := conn.Exec(ctx, "DROP PUBLICATION IF EXISTS pglogrepl_demo;")

	_, err = result.ReadAll()
	if err != nil {
		log.Println("drop publication if exists error", err)
		return
	}

	result = conn.Exec(ctx, "CREATE PUBLICATION pglogrepl_demo FOR ALL TABLES;")

	_, err = result.ReadAll()
	if err != nil {
		log.Println("create publication error", err)

		return
	}

	log.Println("create publication pglogrepl_demo")

	pluginArguments := []string{
		"proto_version '2'",
		"publication_names 'pglogrepl_demo'",
		"messages 'true'",
		"streaming 'true'",
	}

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		log.Println("IdentifySystem failed:", err)
		return
	}

	log.Println("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

	slotName := "pglogrepl_demo"

	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	if err != nil {
		log.Println("CreateReplicationSlot failed:", err)
		return
	}

	log.Println("Created temporary replication slot:", slotName)

	err = pglogrepl.StartReplication(ctx, conn, slotName, sysident.XLogPos, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		log.Println("StartReplication failed:", err)
		return
	}

	log.Println("Logical replication started on slot", slotName)

	clientXLogPos := sysident.XLogPos
	nextStandbyMessageDeadline := time.Now().Add(msgTimeout)
	relationsV2 := map[uint32]*pglogrepl.RelationMessageV2{}
	typeMap := pgtype.NewMap()

	inStream := false

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				log.Println("SendStandbyStatusUpdate failed:", err)
				return
			}

			log.Printf("Sent Standby status message at %s\n", clientXLogPos.String())

			nextStandbyMessageDeadline = time.Now().Add(msgTimeout)
		}

		ctx, cancel := context.WithDeadline(ctx, nextStandbyMessageDeadline)
		rawMsg, err := conn.ReceiveMessage(ctx)

		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}

			log.Println("ReceiveMessage failed:", err)

			return
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			log.Printf("received Postgres WAL error: %+v\n", errMsg)

			return
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			log.Printf("Received unexpected message: %T\n", rawMsg)

			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				log.Println("ParsePrimaryKeepaliveMessage failed:", err)
				return
			}

			log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)

			if pkm.ServerWALEnd > clientXLogPos {
				clientXLogPos = pkm.ServerWALEnd
			}

			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				log.Println("ParseXLogData failed:", err)
				return
			}

			log.Printf("XLogData => WALStart %s ServerWALEnd %s ServerTime %s WALData:\n", xld.WALStart, xld.ServerWALEnd, xld.ServerTime)

			processV2(xld.WALData, relationsV2, typeMap, &inStream)

			if xld.WALStart > clientXLogPos {
				clientXLogPos = xld.WALStart
			}
		}
	}
}

func processV2(walData []byte, relations map[uint32]*pglogrepl.RelationMessageV2, typeMap *pgtype.Map, inStream *bool) {
	logicalMsg, err := pglogrepl.ParseV2(walData, *inStream)
	if err != nil {
		log.Fatalf("Parse logical replication message: %s", err)
	}

	log.Printf("Receive a logical replication message: %s", logicalMsg.Type())

	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		relations[logicalMsg.RelationID] = logicalMsg

	case *pglogrepl.BeginMessage:
		// Indicates the beginning of a group of changes in a transaction. This is only sent for committed transactions. You won't get any events from rolled back transactions.

	case *pglogrepl.CommitMessage:

	case *pglogrepl.InsertMessageV2:
		rel, ok := relations[logicalMsg.RelationID]
		if !ok {
			log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
		}

		values := map[string]interface{}{}

		for idx, col := range logicalMsg.Tuple.Columns {
			colName := rel.Columns[idx].Name

			switch col.DataType {
			case 'n': // null
				values[colName] = nil
			case 'u': // unchanged toast
				// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
			case 't': // text
				val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					log.Fatalln("error decoding column data:", err)
				}

				values[colName] = val
			}
		}

		log.Printf("insert for xid %d\n", logicalMsg.Xid)
		log.Printf("INSERT INTO %s.%s: %v", rel.Namespace, rel.RelationName, values)

	case *pglogrepl.UpdateMessageV2:
		log.Printf("update for xid %d\n", logicalMsg.Xid)
		// ...
	case *pglogrepl.DeleteMessageV2:
		log.Printf("delete for xid %d\n", logicalMsg.Xid)
		// ...
	case *pglogrepl.TruncateMessageV2:
		log.Printf("truncate for xid %d\n", logicalMsg.Xid)
		// ...

	case *pglogrepl.TypeMessageV2:
	case *pglogrepl.OriginMessage:

	case *pglogrepl.LogicalDecodingMessageV2:
		log.Printf("Logical decoding message: %q, %q, %d", logicalMsg.Prefix, logicalMsg.Content, logicalMsg.Xid)

	case *pglogrepl.StreamStartMessageV2:
		*inStream = true

		log.Printf("Stream start message: xid %d, first segment? %d", logicalMsg.Xid, logicalMsg.FirstSegment)
	case *pglogrepl.StreamStopMessageV2:
		*inStream = false

		log.Printf("Stream stop message")
	case *pglogrepl.StreamCommitMessageV2:
		log.Printf("Stream commit message: xid %d", logicalMsg.Xid)
	case *pglogrepl.StreamAbortMessageV2:
		log.Printf("Stream abort message: xid %d", logicalMsg.Xid)
	default:
		log.Printf("Unknown message type in pgoutput stream: %T", logicalMsg)
	}
}

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}

	return string(data), nil
}
