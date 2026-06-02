from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from scraper_recruteAutre import _run_rekrute
from Adzuna_scraper import _run_adzuna
from france_travail_scraper import run_france_travail

app = FastAPI(title="Job Scraper API", description="Scraping d'offres d'emploi Data Engineering")

class ScrapeRequest(BaseModel):
    keywords: str = "data engineer"
    max_results: int = 50

@app.get("/")
def root():
    return {"message": "Job Scraper API is running"}

@app.post("/scrape/rekrute")
def scrape_rekrute(req: ScrapeRequest):
    try:
        result = _run_rekrute(req.keywords, req.max_results)
        return {"status": "success", "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/scrape/adzuna")
def scrape_adzuna(req: ScrapeRequest):
    try:
        result = _run_adzuna(req.keywords, req.max_results)
        return {"status": "success", "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/scrape/france_travail")
def scrape_france_travail(req: ScrapeRequest):
    try:
        result = run_france_travail(req.keywords, req.max_results)
        return {"status": "success", "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)