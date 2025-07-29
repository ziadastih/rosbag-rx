import { ITime } from "../../models/general.models";
import { IChunkInfo, IConnection } from "./ros1-chunk-manager.model";

export interface IBagHeaderInfo {
  chunkCount: number;
  indexPos: number;
  connCount: number;
}
export interface IRos1BagMetadata {
  connections: Map<number, IConnection>;
  chunksInfo: IChunkInfo[];
  startTime: ITime;
  endTime: ITime;
}

export interface IBagHeaderValidation {
  valid: boolean;
  error: string;
}
