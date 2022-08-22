#### Running mymqd dump on Mac

`mymqd dump` uses pcap internally, which requires access to one or more `/dev/bpf*` devices. Since Mac is mostly likely a personal workstation, we recommend the following:

```bash
$ whoami # use the output as `userid`
$ ls -l /dev/bpf* # make a note of username:group for the `bpf*` devices.
$ sudo chown <userid:groupid> /dev/bpf*
```

Should change the ownership of `/dev/bpf*` to current user. Later this can be restored to original (make a note of the original ownership somewhere handy)
