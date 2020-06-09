import csv
import uuid
import psutil
from datetime import datetime, timedelta
from random import gauss, seed, randint, uniform, choice

GAME_NAMES_PATH = "../data/vgsales.csv"

def generate_pc_stats():
	return {
		'CPU': psutil.cpu_percent(interval=1) * 8 * randf(),
		'RAM': psutil.virtual_memory()[2]
	}

def generate_ps4_stats():
	return {
		'CPU': psutil.cpu_percent(interval=1) * 10 * randf(),
		'RAM': psutil.virtual_memory()[2]
	}

supported_platform = {
	'PC': generate_pc_stats(),
	'PS4': generate_ps4_stats()
}

class DataGenerator():
	def __init__(self, num_users=6000000):
		self.uids = self.generate_uids(num_users)
		self.games = self.read_games(GAME_NAMES_PATH)

	def get_users(self):
		return self.uids

	def read_games(self, fname):
		games = []
		with open(fname) as csv_file:
			reader = csv.reader(csv_file)
			for row in reader:
				if row[2] in supported_platform:
					games.append({'game':row[1], 'platform':row[2]})
		return games

	def generate_uids(self, num_users):
		users = [{
			'UID': str(uuid.uuid4()),
			'Age': self.generate_age(),
			'StartedPlaying': None
		} for i in range(num_users)]

		return users

	def get_game(self):
		return choice(self.games)

	def get_uid(self):
		user = choice(self.uids)
		if user['StartedPlaying']:
			delta = datetime.now() - user['StartedPlaying']
			r = gauss(0.5, 0.4)
			if (delta.seconds * delta.days > 30 * 60 and r < 0.8) or r < 0.2:
				user['StartedPlaying'] = None
		else:
			user['StartedPlaying'] = datetime.now()
		return user['UID']

	def generate_age(self):
		return int(gauss(0.75, 0.15) * uniform(22, 70))

	def generate_platform_stats(self, platform):
		return str(supported_platform[platform])

	def generate_data(self):
		uid = self.get_uid()
		time = datetime.now()
		game = self.get_game()
		platform = game['platform']
		platform_stats = self.generate_platform_stats(platform)

		return {
			'UID': uid,
			'Time': time,
			'Game': game['game'],
			'Platform': platform,
			'PlatformStats': platform_stats,
		}

if __name__=="__main__":
	seed()
	generator = DataGenerator(100)
	while True:
		print(generator.generate_data())