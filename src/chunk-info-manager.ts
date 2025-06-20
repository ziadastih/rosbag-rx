import { defer, Observable, of, Subject, takeUntil } from "rxjs";
import {
  extractTime,
  parseMsgDefinition,
  recordDecompression,
  retrieveRecordsFromBuffer,
  shallowRecordRead,
} from "./helper-functions";
import { MessageReaders } from "./message-reader";
import { IRecordShallow } from "./models/general.models";
import { compareTime } from "./utils/timeUtil";
import { Buffer } from "buffer";
import {
  IChunkInfo,
  IConnection,
  IChunkCache,
  IIndexData,
  IIndexDataMsg,
  IRosbagMessage,
  IMsgSchema,
} from "./models/chunk-info-manager.model";
export class ChunkInfoManager {
  private _bagFile: File;
  private _connections: Map<number, IConnection>;
  private _msgSchemas: Map<string, IMsgSchema> = new Map();
  private _readers: MessageReaders = new MessageReaders();
  private _chunkCache: Map<number, IChunkCache> = new Map();
  private _currentCacheBytes = 0;
  private readonly _maxCacheBytes = 50 * 1024 * 1024;
  setFile(file: File) {
    this._bagFile = file;
    this._chunkCache.clear();
    this._currentCacheBytes = 0;
  }

  hasChunk(idx: number): boolean {
    return this._chunkCache.has(idx);
  }

  getCachedChunk(idx: number): IRosbagMessage[] | undefined {
    return this._chunkCache.get(idx)?.messages;
  }

  readChunk$(
    chunkInfo: IChunkInfo,
    nextChunkPos: number,
    connections: Map<number, IConnection>,
    cancel$: Subject<void>
  ): Observable<IRosbagMessage[]> {
    this._connections = connections;
    if (this._chunkCache.has(chunkInfo.idx)) {
      return defer(() => of(this._chunkCache.get(chunkInfo.idx)!.messages));
    }

    return defer(
      () =>
        new Observable<IRosbagMessage[]>((subscriber) => {
          const chunkSlice = this._bagFile.slice(
            chunkInfo.chunkPosition,
            nextChunkPos
          );
          const reader = new FileReader();
          reader.onload = () => {
            try {
              const buffer = Buffer.from(reader.result as ArrayBuffer);
              const messages = this._processChunkBuffer(buffer, chunkInfo);
              const chunkSize =
                chunkInfo.nextChunkPosition - chunkInfo.chunkPosition;
              this._chunkCache.set(chunkInfo.idx, {
                messages,
                size: chunkSize,
              });
              this._currentCacheBytes += chunkSize;
              this._cleanupCache();
              subscriber.next(messages);
              subscriber.complete();
            } catch (err) {
              subscriber.error(err);
            }
          };

          reader.onerror = (e) => subscriber.error(e);
          reader.readAsArrayBuffer(chunkSlice);
          return () => {
            reader.abort();
          };
        })
    ).pipe(takeUntil(cancel$));
  }

  private _cleanupCache(): void {
    while (
      this._currentCacheBytes > this._maxCacheBytes &&
      this._chunkCache.size > 0
    ) {
      const oldestKey = this._chunkCache.keys().next().value;
      const expiredCache = this._chunkCache.get(oldestKey);
      if (expiredCache) {
        this._chunkCache.delete(oldestKey);
        this._currentCacheBytes -= expiredCache.size;
      }
    }
  }

  private _processChunkBuffer(
    buffer: Buffer,
    chunkInfo: IChunkInfo
  ): IRosbagMessage[] {
    const chunk = shallowRecordRead(buffer, chunkInfo.chunkPosition);
    if (!chunk) return [];

    const compression = chunk.recordHeaderFields.compression.toString();
    const chunkSize = chunk.recordHeaderFields.size.readUInt32LE(0);
    const decompressedBuffer = recordDecompression[compression](
      chunk.recordDataBuffer,
      chunkSize
    );
    chunk.recordDataBuffer = decompressedBuffer;

    const indexedDataList = retrieveRecordsFromBuffer(
      buffer.subarray(chunk.recordLength),
      chunkInfo.count,
      chunkInfo.chunkPosition + chunk.recordLength,
      this._parseIndexDataRecord
    );

    const messagePointers: IIndexDataMsg[] = [];
    // todo benchmark this and compare it with flatMap , or forEach with spread operator
    for (let i = 0; i < indexedDataList.length; i++) {
      const msgs = indexedDataList[i].recievedMsgs;
      for (let j = 0; j < msgs.length; j++) {
        messagePointers.push(msgs[j]);
      }
    }
    const sortedPointers = messagePointers.sort((a, b) =>
      compareTime(a.recievedTime, b.recievedTime)
    );
    const rosMessages: IRosbagMessage[] = [];
    for (let i = 0; i < sortedPointers.length; i++) {
      const msgRecord = shallowRecordRead(
        chunk.recordDataBuffer.subarray(sortedPointers[i].msgDataOffset),
        chunk.recordDataOffset
      );
      if (!msgRecord) continue;

      const conn = msgRecord.recordHeaderFields.conn.readUInt32LE(0);
      const connection = this._connections.get(conn);
      if (!connection) continue;

      const time = extractTime(msgRecord.recordHeaderFields.time, 0);
      const { messageDefinition, messageType, topicName } = connection;

      let schema = this._msgSchemas.get(messageType);
      if (!schema) {
        schema = parseMsgDefinition(messageDefinition);
        this._msgSchemas.set(messageType, schema);
      }

      try {
        const parsed = this._readers.readMsg(
          messageType,
          schema,
          msgRecord.recordDataBuffer
        );

        rosMessages.push({
          topic: topicName,
          time,
          data: parsed,
        });
      } catch (err) {
        console.warn(
          `[ParseError] Failed to parse message on topic "${topicName}" at time ${time.sec}.${time.nsec}:`,
          err
        );
      }
    }

    return rosMessages;
  }

  private _parseIndexDataRecord(record: IRecordShallow): IIndexData {
    const { recordDataBuffer, recordHeaderFields, ...rest } = record;
    const count = recordHeaderFields.count.readUint32LE(0);
    return {
      ...rest,
      version: recordHeaderFields.ver.readUint32LE(0),
      conn: recordHeaderFields.conn.readUInt32LE(0),
      count,
      recievedMsgs: Array.from({ length: count }, (_, i) => ({
        recievedTime: extractTime(recordDataBuffer, i * 12),
        msgDataOffset: recordDataBuffer.readUint32LE(i * 12 + 8),
      })),
    };
  }
}
