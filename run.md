# Running the data gatherer

Launch the recorder yourself, in its own terminal, so it survives independently of any tooling session:

```powershell
cd C:\Users\telem\quant\qr-crypto-sim
python recorder.py
```

Leave that window open. It connects spot (BTC/ETH/SOL/BNB-USDT) + perp (BTC-USDT), writes `data/{spot,perp}_<UTCdate>.jsonl.gz`, rotates at UTC midnight, and auto-reconnects on drops.

## Keep it healthy
- **One recorder at a time.** Two processes writing the same daily file corrupts it. Don't start a second.
- **Stop the laptop sleeping** (Settings - Power - Sleep: Never on AC). Sleep is what fragments the logs and creates clock-aligned blind spots in the data.
- **Brief network drops are fine** — they self-heal (fresh snapshot + resync); only ~3 logged gaps on a full day.
- Stop with Ctrl-C (clean shutdown). A hard kill only ever loses the in-flight record.

## Check it's alive
```powershell
Get-Content recorder.log -Tail 5            # should show "connected spot/perp", no repeated errors
(Get-Item data\spot_<UTCdate>.jsonl.gz).Length   # run twice; should grow
```

## Optional: survive logout / auto-start
Register a Scheduled Task that runs at logon (adjust the path):

```powershell
$a = New-ScheduledTaskAction -Execute "python" -Argument "recorder.py" -WorkingDirectory "C:\Users\telem\quant\qr-crypto-sim"
$t = New-ScheduledTaskTrigger -AtLogOn
Register-ScheduledTask -TaskName "qr-recorder" -Action $a -Trigger $t -RunLevel Highest
```
