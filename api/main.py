from fastapi import FastAPI

app = FastAPI()

@app.get("/{message}")
def test(message):
    return {"message": message}