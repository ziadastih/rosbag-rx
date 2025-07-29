// ROS1 CONSTANTS
export const ROS1_MAGIC_WORD_LEN: number = 13;
export const ROS1_MAGIC_WORD: string = "#ROSBAG V2.0\n";
export const ROS1_HEADER_MIN_LEN: number = 8;
export const ROS1_HEADER_PADDING: number = 4096;

// MCAP CONSTANTS
export const MCAP_MAGIC_WORD_LEN = 8;
export const MCAP_MAGIC_WORD = "\x89MCAP0\r\n";
export const MCAP_MAX_HEADER_PADDING = 100; // ? the 4 is the bytes to read the header len
export const MCAP_STD_VARINT_SIZE = 8; //? they use 8 bytes regardless how big is the number
export const MCAP_FOOTER_SIZE = 37; //? op (1) -> recordLen varint (8) -> data (20) -> magic word (8)
