# Пример использования класса KafkaAPI
if name == "__main__":
    kafka_api = KafkaAPI()

    # Создание топика
    kafka_api.create_topic("name")

    # Запись данных в топик (предполагается, что df это pandas DataFrame)
    df = pd.read_csv('name.csv', delimiter=';')
    kafka_api.write_to_topic(df, "market_analysis")

    # Чтение данных из топика
    data_from_kafka = kafka_api.read_from_topic("name")
    print(data_from_kafka.head())

    # Удаление топика
    kafka_api.delete_topic("name")
