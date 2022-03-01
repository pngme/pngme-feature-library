# Sum of balances (latest)

1. Create and activate a virtual environment:

```bash
python3 -m venv .venv
.venv/bin/activate
```

2. Install the dependencies:

```bash
pip3 install -r requirements.txt
```

3. Set the `PNGME_TOKEN` environment variable using your API token from [admin.pngme.com](https://admin.pngme.com):

```bash
export PNGME_TOKEN="eyJraWQiOiJcL3d..."
```

4. Run the example:

```bash
python3 main.py

# 13404
```
