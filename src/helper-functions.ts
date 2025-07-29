import { IMsgFormat, IMsgSchema, ITime } from "./models/general.models";

export function extractTime(buffer: Buffer, offset: number): ITime {
  return {
    sec: buffer.readUInt32LE(offset),
    nsec: buffer.readUInt32LE(offset + 4),
  };
}
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
