import { ITime } from "../models/general.models";

/**
 *
 * @param a the first given time
 * @param b the second given time
 * @returns number : negative if a is earlier b , 0 if both are equal , positive if a is later than b
 */

export function compareTime(a: ITime, b: ITime): number {
  const secDiff = a.sec - b.sec;
  return secDiff || a.nsec - b.nsec;
}

export function isLessThan(a: ITime, b: ITime) {
  return compareTime(a, b) < 0;
}

export function addSecToTime(time: ITime, sec: number): ITime {
  const totalNano = time.nsec + Math.floor((sec % 1) * 1e9);
  const carry = Math.floor(totalNano / 1e9);

  return {
    sec: time.sec + Math.floor(sec) + carry,
    nsec: totalNano % 1e9,
  };
}
