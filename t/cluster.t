use Test::Most;
use RedisDB::Cluster;

subtest crc16 => sub {
    is RedisDB::Cluster::crc16("123456789"), 0x31c3,
      "correct CRC16 for 123456789";
    dies_ok { RedisDB::Cluster::crc16("abc\x{300}"); }
    "can't compute crc for string with wide characters";
};

subtest "key slot" => sub {
    is RedisDB::Cluster::key_slot("123456789"), 0x31c3,
      "correct key slot for 123456789";
    is RedisDB::Cluster::key_slot("12{345}6789"),
      RedisDB::Cluster::key_slot("foo{345}boo"),
      "keys with the same hash tag belong to the same slot";
    is RedisDB::Cluster::key_slot("foo{}bar"),
      RedisDB::Cluster::crc16("foo{}bar") & 16383,
      "if hash tag is empty whole key is hashed";
};

done_testing;
