#!/usr/bin/env perl
use 5.020;
use warnings;
use RedisDB;

my ($host, $port) = @ARGV;

my $redis = RedisDB->new(
    host => $host,
    port => $port,
);

my $commands = $redis->command;
my %commands;
for (@$commands) {
    my ($cmd, $arity, $flags, $first_key, $last_key, $step_key) = @$_;
    next unless $first_key;
    $commands{$cmd} = $first_key;
}

for (sort keys %commands) {
    say "    $_ => $commands{$_}";
}
