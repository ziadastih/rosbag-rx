import {
  BehaviorSubject,
  catchError,
  defer,
  EMPTY,
  filter,
  map,
  Observable,
  shareReplay,
  Subject,
  switchMap,
  takeUntil,
} from "rxjs";
import {
  ROS1_HEADER_MIN_LEN,
  ROS1_HEADER_PADDING,
  ROS1_MAGIC_WORD,
  ROS1_MAGIC_WORD_LEN,
} from "../utils/bag-format-constants";
import {
  extractFields,
  getField,
  retrieveRecordsFromBuffer,
} from "./ros1-helper-functions";
import { compareTime } from "../utils/timeUtil";

import { Buffer } from "buffer";
import int53 from "int53";
import {
  IBagHeaderInfo,
  IBagHeaderValidation,
  IRos1BagMetadata,
} from "./models/ros1-bag-inspector.model";
import { IChunkInfo, IConnection } from "./models/ros1-chunk-manager.model";
import { IRecordShallow } from "./models/ros1-general.models";
import { extractTime } from "../helper-functions";
export class Ros1BagInspector {
  private _bagFile$ = new BehaviorSubject<File | null>(null);
  private _destroyInstance$ = new Subject<boolean>();
  private _error$ = new Subject<string>();

  get bagMetadata$(): Observable<IRos1BagMetadata> {
    return this._bagFile$.pipe(
      takeUntil(this._destroyInstance$),
      filter((file): file is File => !!file),
      switchMap((file) =>
        this._readHeader$(file).pipe(
          catchError((err) => {
            this._error$.next(err);
            return EMPTY;
          }),
          switchMap((headerInfo) =>
            this._extractFileMetadata$(file, headerInfo).pipe(
              catchError((err) => {
                this._error$.next(err);
                return EMPTY;
              })
            )
          )
        )
      ),
      shareReplay(1)
    );
  }

  get error$() {
    return this._error$.asObservable().pipe(takeUntil(this._destroyInstance$));
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
          reader.readAsArrayBuffer(file.slice(0, ROS1_HEADER_PADDING));
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
    const magicWordBuffer = headerBuffer.subarray(0, ROS1_MAGIC_WORD_LEN);
    const totalBufferLength = headerBuffer.length;
    if (magicWordBuffer.toString() !== ROS1_MAGIC_WORD) {
      return {
        valid: false,
        error: `Invalid ROS bag file: Magic string mismatch. Expected ${ROS1_MAGIC_WORD} but found '${magicWordBuffer.toString()}'. The file might be corrupted or not a valid ROS bag.`,
      };
    }
    if (totalBufferLength < ROS1_MAGIC_WORD_LEN + ROS1_HEADER_MIN_LEN) {
      return {
        valid: false,
        error: `Invalid ROS bag file: The file is smaller than the minimum required size to extract the header and data lengths. This may indicate that the file is corrupted or not a valid ROS bag.`,
      };
    }
    const headerLength = headerBuffer.readInt32LE(ROS1_MAGIC_WORD_LEN);
    if (
      totalBufferLength <
      ROS1_MAGIC_WORD_LEN + ROS1_HEADER_MIN_LEN + headerLength
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
    const headerLength = headerBuffer.readInt32LE(ROS1_MAGIC_WORD_LEN);
    const headerFieldOffset = ROS1_MAGIC_WORD_LEN + 4; // 17
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
  ): Observable<IRos1BagMetadata> {
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
        if (connections.length === 0) throw new Error("No connections found");
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
        if (chunksInfo.length === 0) throw new Error("No chunks found");
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
