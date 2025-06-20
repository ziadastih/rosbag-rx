import { IChunkInfo, IConnection } from "./chunk-info-manager.model";
import { ITime } from "./general.models";

export interface IBagHeaderInfo {
  chunkCount: number;
  indexPos: number;
  connCount: number;
}
export interface IBagMetadata {
  connections: Map<number, IConnection>;
  chunksInfo: IChunkInfo[];
  startTime: ITime;
  endTime: ITime;
}

export interface IBagHeaderValidation {
  valid: boolean;
  error: string;
}
