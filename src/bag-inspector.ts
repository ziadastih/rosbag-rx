import {
  BehaviorSubject,
  defer,
  filter,
  map,
  Observable,
  shareReplay,
  Subject,
  switchMap,
  takeUntil,
} from "rxjs";
import {
  extractFields,
  extractTime,
  getField,
  retrieveRecordsFromBuffer,
} from "./helper-functions";
import {
  FORMAT_VERSION,
  FORMAT_VERSION_OFFSET,
  HEADER_MIN_LEN,
  HEADER_PADDING,
} from "./utils/bag-format-constants";
import { Buffer } from "buffer";
import int53 from "int53";
import { IRecordShallow } from "./models/general.models";
import { compareTime } from "./utils/timeUtil";
import {
  IBagHeaderInfo,
  IBagHeaderValidation,
  IBagMetadata,
} from "./models/bag-inspector.model";
import { IChunkInfo, IConnection } from "./models/chunk-info-manager.model";

export class BagInspector {
  private _bagFile$ = new BehaviorSubject<File | null>(null);
  private _destroyInstance$ = new Subject<boolean>();
  get bagMetadata$(): Observable<IBagMetadata> {
    return this._bagFile$.pipe(
      takeUntil(this._destroyInstance$),
      filter((file): file is File => !!file),
      switchMap((file) =>
        this._readHeader$(file).pipe(
          switchMap((headerInfo) =>
            this._extractFileMetadata$(file, headerInfo)
          )
        )
      ),
      shareReplay(1)
    );
  }

  setFile(file: File): void {
    this._bagFile$.next(file);
  }

  destroyInstance() {
    this._destroyInstance$.next(true);
    this._destroyInstance$.complete();
  }
  //#region Bag Header
  private _readHeader$(file: File): Observable<IBagHeaderInfo> {
    return defer(
      () =>
        new Observable<ArrayBuffer>((subscriber) => {
          const reader = new FileReader();
          reader.onload = () => subscriber.next(reader.result as ArrayBuffer);
          reader.onerror = (e) => subscriber.error(e);
          reader.readAsArrayBuffer(file.slice(0, HEADER_PADDING));
          return () => {
            reader.abort();
          };
        })
    ).pipe(
      takeUntil(this._destroyInstance$),
      map((buffer) => {
        const headerBuffer = Buffer.from(buffer);
        const validation = this._verifyHeader(headerBuffer);
        if (!validation.valid) throw new Error(validation.error);
        return this._parseBagHeader(headerBuffer);
      })
    );
  }

  private _verifyHeader(headerBuffer: Buffer): IBagHeaderValidation {
    const magicWordBuffer = headerBuffer.subarray(0, FORMAT_VERSION_OFFSET);
    const totalBufferLength = headerBuffer.length;
    if (magicWordBuffer.toString() !== FORMAT_VERSION) {
      return {
        valid: false,
        error: `Invalid ROS bag file: Magic string mismatch. Expected ${FORMAT_VERSION} but found '${magicWordBuffer.toString()}'. The file might be corrupted or not a valid ROS bag.`,
      };
    }
    if (totalBufferLength < FORMAT_VERSION_OFFSET + HEADER_MIN_LEN) {
      return {
        valid: false,
        error: `Invalid ROS bag file: The file is smaller than the minimum required size to extract the header and data lengths. This may indicate that the file is corrupted or not a valid ROS bag.`,
      };
    }
    const headerLength = headerBuffer.readInt32LE(FORMAT_VERSION_OFFSET);
    if (
      totalBufferLength <
      FORMAT_VERSION_OFFSET + HEADER_MIN_LEN + headerLength
    ) {
      return {
        valid: false,
        error: `Invalid ROS bag file: The Header is larger than the essential padding format. This may indicate that the file is corrupted or not a valid ROS bag.`,
      };
    }
    return {
      valid: true,
      error: "",
    };
  }

  private _parseBagHeader(headerBuffer: Buffer): IBagHeaderInfo {
    const headerLength = headerBuffer.readInt32LE(FORMAT_VERSION_OFFSET);
    const headerFieldOffset = FORMAT_VERSION_OFFSET + 4; // 17
    const headerFieldsBuffer = headerBuffer.subarray(
      headerFieldOffset,
      headerFieldOffset + headerLength
    );
    const headerFields = extractFields(headerFieldsBuffer);
    if (!headerFields) throw new Error(`Bag header field Missing equal '='`);
    return {
      indexPos: int53.readUInt64LE(headerFields["index_pos"], 0),
      connCount: headerFields["conn_count"]!.readInt32LE(0),
      chunkCount: headerFields["chunk_count"]!.readInt32LE(0),
    };
  }
  //#endregion

  //#region  Bag Metadata
  private _extractFileMetadata$(
    file: File,
    bagHeaderInfo: IBagHeaderInfo
  ): Observable<IBagMetadata> {
    const { connCount, chunkCount, indexPos } = bagHeaderInfo;
    return defer(
      () =>
        new Observable<ArrayBuffer>((subscriber) => {
          const reader = new FileReader();
          reader.onload = () => subscriber.next(reader.result as ArrayBuffer);
          reader.onerror = (e) => subscriber.error(e);
          reader.readAsArrayBuffer(file.slice(indexPos, file.size));
          return () => {
            reader.abort();
          };
        })
    ).pipe(
      takeUntil(this._destroyInstance$),
      map((buffer) => {
        const bufferResult = Buffer.from(buffer);

        const connections = retrieveRecordsFromBuffer<IConnection>(
          bufferResult,
          connCount,
          indexPos,
          this._parseConnectionRecord
        );

        const connectionsMap = new Map(
          connections.map((connection) => [connection.conn, connection])
        );

        const lastConnection = connections[connCount - 1];
        const chunksInfoOffset =
          lastConnection.recordOffset + lastConnection.recordLength;

        let chunksInfo = retrieveRecordsFromBuffer<IChunkInfo>(
          bufferResult.subarray(chunksInfoOffset - indexPos),
          chunkCount,
          chunksInfoOffset,
          this._parseChunkInfo
        );

        chunksInfo = chunksInfo
          .map((chunk, i) => ({
            ...chunk,
            nextChunkPosition: chunksInfo[i + 1]?.chunkPosition || file.size,
          }))
          .sort((a, b) => compareTime(a.startTime, b.startTime))
          .map((chunk, idx) => ({ ...chunk, idx }));

        return {
          startTime: chunksInfo[0].startTime,
          endTime: chunksInfo[chunksInfo.length - 1].endTime,
          chunksInfo,
          connections: connectionsMap,
        };
      })
    );
  }

  private _parseConnectionRecord(
    shallowRecordData: IRecordShallow
  ): IConnection {
    const { recordDataBuffer, recordHeaderFields, ...rest } = shallowRecordData;
    const dataFields = extractFields(recordDataBuffer);
    if (!dataFields) return undefined;
    const conn = recordHeaderFields.conn.readUInt32LE(0);
    const topicName = recordHeaderFields.topic.toString();
    const messageType = getField(dataFields, "type");
    const md5sum = getField(dataFields, "md5sum");
    const messageDefinition = getField(dataFields, "message_definition"); //ignore theses for now unless they are breaking
    return {
      ...rest,
      conn,
      topicName,
      messageType,
      md5sum,
      messageDefinition,
    };
  }

  private _parseChunkInfo(shallowRecordData: IRecordShallow): IChunkInfo {
    const { recordDataBuffer, recordHeaderFields, ...rest } = shallowRecordData;
    const count = recordHeaderFields.count.readUInt32LE(0);
    return {
      ...rest,
      version: recordHeaderFields?.ver?.readUint32LE(0),
      chunkPosition: int53.readUInt64LE(recordHeaderFields.chunk_pos, 0),
      startTime: extractTime(recordHeaderFields.start_time, 0),
      endTime: extractTime(recordHeaderFields.end_time, 0),
      count,
      connections: Array.from({ length: count }, (_, i) => ({
        conn: recordDataBuffer.readUInt32LE(i * 8),
        count: recordDataBuffer.readUInt32LE(i * 8 + 4),
      })),
      idx: 0,
      nextChunkPosition: undefined,
    };
  }

  //#endregion
}
