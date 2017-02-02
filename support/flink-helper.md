# Useful Flink scripts

## Kill all task managers

```
for i in {2..9}; do ssh power$i "pkill -f flink"; done
```

