import * as LZ4 from "lz4js";
import { Buffer } from "buffer";
import { decode } from "seek-bzip";
import { IRecordFields, IRecordShallow } from "./models/ros1-general.models";

if (typeof globalThis.Buffer === "undefined") {
  (globalThis as any).Buffer = Buffer;
}

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

export const recordDecompression = {
  none: (buffer: Buffer, size: number) => buffer,
  lz4: (buffer: Buffer, size: number) =>
    Buffer.from(LZ4.decompress(buffer, size)),
  bz2: (buffer: Buffer, size: number) => Buffer.from(decode(buffer)),
};
