import int53 from "int53";
import { extractTime } from "./helper-functions";
import { IMsgFormat, IMsgSchema } from "./models/chunk-info-manager.model";

export class StdTypeReader {
  buffer: Buffer;
  offset: number;
  view: DataView;

  parsingMethods = {
    string: () => {
      const strLen = this.buffer.readInt32LE(this.offset);
      this.offset += 4;
      const string = this.buffer.toString(
        "ascii",
        this.offset,
        this.offset + strLen
      );
      this.offset += strLen;
      return string;
    },
    bool: () => {
      return this.view.getUint8(this.offset++) !== 0;
    },

    int8: () => {
      return this.view.getInt8(this.offset++);
    },

    uint8: () => {
      return this.view.getUint8(this.offset++);
    },
    int16: () => {
      const result = this.view.getInt16(this.offset, true);
      this.offset += 2;
      return result;
    },

    uint16: () => {
      const result = this.view.getUint16(this.offset, true);
      this.offset += 2;
      return result;
    },

    int32: () => {
      const result = this.view.getInt32(this.offset, true);
      this.offset += 4;
      return result;
    },

    uint32: () => {
      const result = this.view.getUint32(this.offset, true);
      this.offset += 4;
      return result;
    },
    float32: () => {
      const result = this.view.getFloat32(this.offset, true);
      this.offset += 4;
      return result;
    },
    float64: () => {
      const result = this.view.getFloat64(this.offset, true);
      this.offset += 8;
      return result;
    },
    int64: () => {
      const data = int53.readInt64LE(this.buffer, this.offset);
      this.offset += 8;
      return data;
    },
    uint64: () => {
      const data = int53.readUInt64LE(this.buffer, this.offset);
      this.offset += 8;
      return data;
    },
    time: () => {
      const time = extractTime(this.buffer, this.offset);
      this.offset += 8;
      return time;
    },
    duration: () => {
      const time = extractTime(this.buffer, this.offset);
      this.offset += 8;
      return time;
    },
    json: () => {},
    byte: () => {
      return this.view.getInt8(this.offset++);
    },
    char: () => {
      return this.view.getUint8(this.offset++);
    },
  };
  constructor(buffer: Buffer) {
    this.buffer = buffer;
    this.offset = 0;
    this.view = new DataView(buffer.buffer, buffer.byteOffset);
  }
}

export class MessageReaders {
  private msgTypesParsers = new Map<string, Function>();
  private _schemas = new Map<string, boolean>();
  msg = {};
  result = {};

  private _createParser = (format: IMsgFormat) => {
    const cachedParser = this.msgTypesParsers.get(format.keyType);
    if (cachedParser) {
      return cachedParser;
    }
    const formatFields = format.nestedKeys;
    const parser = (reader: StdTypeReader) => {
      this.result = {};
      for (let i = 0; i < formatFields.length; i++) {
        const field = formatFields[i];
        this.result[field.key] = this.parseField(reader, field);
      }
      return this.result;
    };
    this.msgTypesParsers.set(format.keyType, parser);
    return parser;
  };

  private parseField = (reader: StdTypeReader, field: IMsgFormat) => {
    const parsingMethods = reader.parsingMethods;
    const stdType = parsingMethods[field.keyType];

    if (field.constantValue) {
      return field.constantValue;
    }

    if (field.isArray) {
      const arrLength = field.arrayLength || parsingMethods.uint32();
      const result = new Array(arrLength);
      for (let j = 0; j < arrLength; j++) {
        result[j] = stdType
          ? stdType()
          : (
              this.msgTypesParsers.get(field.keyType) ||
              this._createParser(field)
            )(reader);
      }
      return result;
    }

    return stdType
      ? stdType()
      : (this.msgTypesParsers.get(field.keyType) || this._createParser(field))(
          reader
        );
  };

  readMsg = (msgType: string, schema: IMsgSchema, buffer: Buffer) => {
    const reader = new StdTypeReader(buffer);
    if (!this._schemas.get(msgType)) {
      schema.MSGSTypes.forEach((msgType) => {
        if (!this.msgTypesParsers.has(msgType.keyType)) {
          this._createParser(msgType);
        }
      });
      this._schemas.set(msgType, true);
    }

    this.msg = {};
    for (let i = 0; i < schema.topLevelKeys.length; i++) {
      const format = schema.topLevelKeys[i];
      this.msg[format.key] = this.parseField(reader, format);
    }

    return this.msg;
  };
}
