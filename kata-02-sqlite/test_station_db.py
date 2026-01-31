import station_db

def main():
    c = station_db.get_connection()
    station_db.create_tables(c)
    station_db.seed_stations(c)
    station_db.seed_observations(c)
    print('JOIN:', station_db.get_station_observations(c))
    station_db.update_station(c, 'AMA001', name='Amarillo Updated')
    print('UPDATED:', station_db.get_station(c, 'AMA001'))
    station_db.delete_station(c, 'DEN001')
    print('DELETED:', station_db.get_station(c, 'DEN001'))

if __name__ == "__main__":
    main()
