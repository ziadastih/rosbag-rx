import { IRecordFields, IRecordShallow, ITime } from "./models/general.models";
import * as LZ4 from "lz4js";
import { Buffer } from "buffer";
import { IMsgFormat, IMsgSchema } from "./models/chunk-info-manager.model";

export const extractFields = (buffer: Buffer): IRecordFields | undefined => {
  let offset = 0;
  const headerFields: IRecordFields = {};
  while (offset < buffer.length) {
    const fieldLen = buffer.readInt32LE(offset);
    offset += 4;

    const fieldData = buffer.subarray(offset, offset + fieldLen);
    const equalCharIndex = fieldData.indexOf("=".charCodeAt(0));
    if (equalCharIndex === -1) {
      return undefined;
    }
    const fieldName = fieldData.subarray(0, equalCharIndex).toString();
    const fieldBufferValue = fieldData.subarray(equalCharIndex + 1);
    headerFields[fieldName] = fieldBufferValue;
    offset += fieldLen;
  }

  return headerFields;
};

/**
 * this function takes a buffer and a starting point , analyze it and return the buffer section in details
 * @param recordBuffer the buffer representing the actual record
 * @param initialOffset the starting point in reference to the file and not the current buffer
 * @returns a record of the record start , length , fields(buffer) , data(buffer)
 */

export const shallowRecordRead = (
  recordBuffer: Buffer,
  initialOffset: number
): IRecordShallow | undefined => {
  const recordHeaderLen = recordBuffer.readInt32LE(0);
  const headerFieldsBuffer = recordBuffer.subarray(4, 4 + recordHeaderLen);
  const recordHeaderFields = extractFields(headerFieldsBuffer);
  if (!recordHeaderFields) return undefined;
  const recordDataLength = recordBuffer.readInt32LE(recordHeaderLen + 4);
  const headerOffset = 4 + recordHeaderLen + 4;
  const recordLength = recordDataLength + headerOffset;
  const recordDataBuffer = recordBuffer.subarray(headerOffset, recordLength); // buffer without the header

  return {
    recordOffset: initialOffset,
    recordLength,
    recordDataOffset: initialOffset + headerOffset,
    recordHeaderFields,
    recordDataBuffer,
  };
};

export const retrieveRecordsFromBuffer = <T>(
  buffer: Buffer,
  recordsCount: number,
  startingOffset: number,
  recordParser: (data: IRecordShallow) => T
): T[] => {
  const records: T[] = []; // allocate count for array
  let bufferOffset = 0;

  for (let i = 0; i < recordsCount; i++) {
    const currentBuffer = buffer.subarray(bufferOffset);
    const shallowData = shallowRecordRead(
      currentBuffer,
      startingOffset + bufferOffset
    );
    const parsedData = recordParser(shallowData) as T;

    bufferOffset += shallowData.recordLength;
    records.push(parsedData);
  }
  return records;
};

export const getField = (
  fieldsRecord: IRecordFields,
  fieldName: string
): string | undefined => {
  if (!fieldsRecord[fieldName]) {
    return undefined;
  }

  return fieldsRecord[fieldName].toString();
};

export function extractTime(buffer: Buffer, offset: number): ITime {
  return {
    sec: buffer.readUInt32LE(offset),
    nsec: buffer.readUInt32LE(offset + 4),
  };
}

export const recordDecompression = {
  none: (buffer: Buffer, size: number) => buffer,
  lz4: (buffer: Buffer, size: number) =>
    Buffer.from(LZ4.decompress(buffer, size)),
};

/**
 * function that takes a msg definition and creates a format from it so we can parse the msg and locate the data of each key
 * @param msgDef
 *
 */

export const parseMsgDefinition = (msgDef: string): IMsgSchema => {
  const linesArr = msgDef.split("\n");

  const filteredLines: string[] = [];
  for (let i = 0; i < linesArr.length; i++) {
    const line = linesArr[i].trim();
    if (line.startsWith("#") || !line || line.startsWith("==")) continue;
    filteredLines.push(line);
  }

  const msgSchema = {
    topLevelKeys: [],
    MSGSTypes: new Map(),
  };

  let currentMSG: IMsgFormat = {
    key: "",
    keyType: "",
    isArray: false,
    nestedKeys: [],
    constantValue: undefined,
    arrayLength: undefined,
  };
  for (let i = 0; i < filteredLines.length; i++) {
    let [keyType, key, ...rest]: string[] = filteredLines[i]
      .trim()
      .split(/\s+/);
    let [equalSign, constantValue] = rest;
    if (equalSign !== "=") {
      constantValue = undefined;
    }

    if (key.includes("=")) {
      let split = key.split("=");
      key = split[0];
      constantValue = split[1];
    }

    const arrayRegex = /^(.+)\[(\d*)\]$/;
    const arrayMatch = keyType.match(arrayRegex);
    let isArray = false;
    let arrayLength: number | undefined = undefined;

    // check if we have an array and extract the type / length if available
    if (arrayMatch) {
      isArray = true;
      keyType = arrayMatch[1];
      if (arrayMatch[2]) {
        arrayLength = parseInt(arrayMatch[2], 10);
      }
    }

    let splitType = keyType.split("/");
    keyType = splitType[splitType.length - 1].toLowerCase();

    const lineFormat = {
      key,
      keyType,
      isArray,
      nestedKeys: [],
      constantValue,
      arrayLength,
    };

    // check if the msg starts with MSG => reached line which describes the key of an obj msg
    if (keyType === "msg:") {
      // if we have an actual key means we finished the prev MSG so we push it and starts a new one

      if (currentMSG.key) {
        const formattedMsgKey = currentMSG.key.split("/");
        currentMSG.keyType =
          formattedMsgKey[formattedMsgKey.length - 1].toLowerCase();
        msgSchema.MSGSTypes.set(
          formattedMsgKey[formattedMsgKey.length - 1].toLowerCase(),
          currentMSG
        );
      }
      currentMSG = lineFormat;
      continue;
    }

    if (currentMSG.key) {
      // if we have an actual MSG means this line refer to a key inside this MSG so we push it
      currentMSG.nestedKeys.push(lineFormat);
      // if we reached the end and we have an actual MSG we need to push it
      if (i === filteredLines.length - 1) {
        const formattedMsgKey = currentMSG.key.split("/");
        currentMSG.keyType =
          formattedMsgKey[formattedMsgKey.length - 1].toLowerCase();
        msgSchema.MSGSTypes.set(
          formattedMsgKey[formattedMsgKey.length - 1].toLowerCase(),
          currentMSG
        );
      }

      continue;
    }
    msgSchema.topLevelKeys.push(lineFormat);
  }
  return msgSchema;
};
