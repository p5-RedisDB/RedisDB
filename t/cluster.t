use Test::Most;
use RedisDB::Cluster;

subtest crc16 => sub {
    is RedisDB::Cluster::crc16("123456789"), 0x31c3,
      "correct CRC16 for 123456789";
    dies_ok { RedisDB::Cluster::crc16("abc\x{300}"); }
    "can't compute crc for string with wide characters";
};

done_testing;
