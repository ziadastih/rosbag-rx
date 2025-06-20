import { IRecord as IRecord, ITime } from "./general.models";

export interface IRosbagMessage {
  topic: string;
  time: ITime;
  data: any;
}

export interface IChunkCache {
  messages: IRosbagMessage[];
  size: number;
}
export interface IConnection extends IRecord {
  conn: number;
  topicName: string;
  messageType: string;
  md5sum: string;
  messageDefinition: string;
}

export interface IChunkInfo extends IRecord {
  version: number;
  chunkPosition: number;
  startTime: ITime;
  endTime: ITime;
  count: number;
  connections: IChunkConnection[];
  idx: number;
  nextChunkPosition: number;
}
export interface IChunkConnection {
  conn: number;
  count: number;
}

export interface IChunk extends IRecord {
  chunkCompression: string; // 'lz4'  | 'none' | 'bz2'
  chunkSize: number;
}

export interface IIndexData extends IRecord {
  version: number;
  conn: number;
  count: number;
  recievedMsgs: IIndexDataMsg[];
}

export interface IIndexDataMsg {
  recievedTime: ITime;
  msgDataOffset: number;
}

export interface IMsgFormat {
  key: string;
  keyType: string;
  isArray: boolean;
  nestedKeys: IMsgFormat[];
  constantValue: undefined | string;
  arrayLength: undefined | number;
}
export interface IMsgSchema {
  topLevelKeys: IMsgFormat[];
  MSGSTypes: Map<string, IMsgFormat>;
}
