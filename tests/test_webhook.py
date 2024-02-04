import httpx


def test_webhook():
    with httpx.Client() as client:
        response = client.post("http://127.0.0.1:8000/new-update?title=Dick%20raiser%20is%20coming&message=For%20your"
                               "%20ass")
    return response

test_webhook()
