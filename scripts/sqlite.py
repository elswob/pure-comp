import sqlite3

def main():
	create_db()

def create_db():
	conn = sqlite3.connect('pure_comp.db')
	c = conn.cursor()

	# Create table
	c.execute('''CREATE TABLE stocks (person_id real, publication_id real, symbol text, qty real, price real)''')

	# Insert a row of data
	c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")

	# Save (commit) the changes
	conn.commit()

	# We can also close the connection if we are done with it.
	# Just be sure any changes have been committed or they will be lost.
	conn.close()

if __name__ == '__main__':
	main()