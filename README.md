import os
import json
import sqlite3
import requests
import openai
import random
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtWidgets import QApplication, QWidget, QVBoxLayout, QPushButton, QTextEdit, QProgressBar, QLabel
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from PIL import Image
from io import BytesIO
from time import sleep
import base64

# Load environment variables from .env file
load_dotenv()

# Set up directories
DESKTOP_PATH = os.path.join(os.path.expanduser("~"), "Desktop", "DigimonScraper")
DATA_FOLDER = os.path.join(DESKTOP_PATH, "data")
IMAGES_FOLDER = os.path.join(DATA_FOLDER, "images")
LOGS_FOLDER = os.path.join(DESKTOP_PATH, "logs")
DB_PATH = os.path.join(DATA_FOLDER, "digimon.db")
LOG_FILE = os.path.join(LOGS_FOLDER, "scraper.log")

os.makedirs(DATA_FOLDER, exist_ok=True)
os.makedirs(IMAGES_FOLDER, exist_ok=True)
os.makedirs(LOGS_FOLDER, exist_ok=True)

# Load OpenAI API keys from .env file
openai_api_keys = [os.getenv(f"OPENAI_API_KEY_{i}") for i in range(1, 9) if os.getenv(f"OPENAI_API_KEY_{i}")]
if not openai_api_keys:
    raise ValueError("No OpenAI API keys found in .env file.")

api_key_index = 0

def get_next_api_key():
    """Rotates OpenAI API keys."""
    global api_key_index
    key = openai_api_keys[api_key_index]
    api_key_index = (api_key_index + 1) % len(openai_api_keys)
    return key

# User-Agent rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0"
]

def get_random_user_agent():
    return random.choice(USER_AGENTS)

# Retry decorator for requests
def retry_request(func):
    def wrapper(*args, **kwargs):
        retries = 3
        while retries > 0:
            try:
                return func(*args, **kwargs)
            except requests.RequestException as e:
                retries -= 1
                if retries == 0:
                    raise e
                sleep(3)  # Wait before retrying
    return wrapper

class ScraperThread(QThread):
    progress_signal = pyqtSignal(int)
    log_signal = pyqtSignal(str)

    def __init__(self):
        super().__init__()

    def run(self):
        self.scrape_data()

    def log_message(self, message):
        self.log_signal.emit(message)

    def scrape_data(self):
        """Scrapes data from Digimon list and cards, then downloads images."""
        try:
            digimons = self.get_digimon_list()
            card_digimons = self.get_digimon_cards()
            total_digimons = len(digimons) + len(card_digimons)
            self.progress_signal.emit(0)
            
            all_digimons = digimons + card_digimons
            for i, digimon in enumerate(all_digimons):
                self.progress_signal.emit(int((i + 1) / total_digimons * 100))
                self.scrape_digimon_details(digimon)
        except Exception as e:
            self.log_message(f"Error: {e}")

    @retry_request
    def get_digimon_list(self):
        """Scrapes the Digimon list from the Fandom page."""
        url = "https://digimon.fandom.com/wiki/List_of_Digimon"
        response = requests.get(url, headers={"User-Agent": get_random_user_agent()}, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        digimon_links = soup.select("table a[href^='/wiki/']")
        return [{"name": link.text.strip(), "url": "https://digimon.fandom.com" + link["href"]} for link in digimon_links if link.text.strip()]

    @retry_request
    def get_digimon_cards(self):
        """Scrapes Digimon card data from the Fandom page."""
        url = "https://digimon.fandom.com/wiki/List_of_Digimon_Cards"
        response = requests.get(url, headers={"User-Agent": get_random_user_agent()}, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        card_links = soup.select("table a[href^='/wiki/']")
        return [{"name": link.text.strip(), "url": "https://digimon.fandom.com" + link["href"]} for link in card_links if link.text.strip()]

    @retry_request
    def scrape_digimon_details(self, digimon):
        """Scrapes individual Digimon details and stores in database and image folder."""
        try:
            response = requests.get(digimon["url"], headers={"User-Agent": get_random_user_agent()})
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Scrape and process data
            name = digimon["name"]
            description = self.get_description(soup)
            image_url = self.get_image_url(soup)
            image = self.download_image(image_url)
            
            # Save data in database and image
            self.save_to_db(name, description, image)
            self.save_image(name, image)

            self.log_message(f"Scraped {name} successfully.")
        except Exception as e:
            self.log_message(f"Error scraping {digimon['name']}: {e}")

    def get_description(self, soup):
        """Extracts the description of the Digimon."""
        description_tag = soup.find("div", class_="pi-item pi-data pi-item-spacing pi-border-color")
        if description_tag:
            return description_tag.get_text(strip=True)
        return "No description available."

    def get_image_url(self, soup):
        """Extracts the image URL of the Digimon."""
        image_tag = soup.find("img", class_="pi-image-thumbnail")
        return image_tag["src"] if image_tag else ""

    def download_image(self, image_url):
        """Downloads the image from the provided URL."""
        if not image_url:
            return None
        
        response = requests.get(image_url)
        return Image.open(BytesIO(response.content)) if response.status_code == 200 else None

    def save_to_db(self, name, description, image):
        """Saves the Digimon data to the SQLite database."""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS digimon (name TEXT, description TEXT)''')
        
        # Check if the digimon already exists before inserting
        cursor.execute("SELECT 1 FROM digimon WHERE name=?", (name,))
        if cursor.fetchone() is None:
            cursor.execute("INSERT INTO digimon (name, description) VALUES (?, ?)", (name, description))
            conn.commit()
        
        conn.close()

    def save_image(self, name, image):
        """Saves the Digimon image to the image folder."""
        if image:
            image_path = os.path.join(IMAGES_FOLDER, f"{name}.png")
            image.save(image_path)

# PyQt6 GUI
class DigimonScraperApp(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        layout = QVBoxLayout()

        self.log_box = QTextEdit(self)
        self.log_box.setReadOnly(True)
        layout.addWidget(self.log_box)

        self.scrape_label = QLabel("Scraping Progress:", self)
        layout.addWidget(self.scrape_label)
        self.scrape_progress = QProgressBar(self)
        self.scrape_progress.setRange(0, 100)
        layout.addWidget(self.scrape_progress)

        self.scrape_button = QPushButton("Start Scraper", self)
        self.scrape_button.clicked.connect(self.run_scraper)
        layout.addWidget(self.scrape_button)

        self.exit_button = QPushButton("Exit", self)
        self.exit_button.clicked.connect(self.close)
        layout.addWidget(self.exit_button)

        self.setLayout(layout)
        self.setWindowTitle("Digimon Scraper")
        self.resize(600, 400)

    def log_message(self, message):
        """Logs messages to the UI."""
        self.log_box.append(message)
        print(message)

    def run_scraper(self):
        """Starts the scraper."""
        self.scraper_thread = ScraperThread()
        self.scraper_thread.progress_signal.connect(self.scrape_progress.setValue)
        self.scraper_thread.log_signal.connect(self.log_message)
        self.scraper_thread.start()

def main():
    app = QApplication(sys.argv)
    scraper_app = DigimonScraperApp()
    scraper_app.show()
    sys.exit(app.exec())

if __name__ == '__main__':
    main()