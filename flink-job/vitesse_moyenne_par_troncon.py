from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.stream_execution_environment import StreamExecutionEnvironment
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common import WatermarkStrategy, Time
from pyflink.common.typeinfo import Types
import os

# Obtenez le chemin absolu du répertoire actuel du script
script_directory = os.path.dirname(os.path.abspath(__file__))
# Construisez le chemin relatif vers le fichier JAR
jar_file_path = os.path.join(script_directory, 'flink-sql-connector-kafka-1.17.2.jar')

# Ajoutez le fichier JAR en tant que dépendance
env = StreamExecutionEnvironment.get_execution_environment()
env.add_jars('file://' + jar_file_path)

source = KafkaSource.builder() \
    .set_bootstrap_servers("localhost:9092") \
    .set_topics("vitesse_moyenne_localisation") \
    .set_group_id("vitesse_moyenne_localisation_flink") \
    .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
    .set_value_only_deserializer(
        JsonRowDeserializationSchema.builder().type_info(Types.ROW_NAMED(
            ['key', 'averageVehicleSpeed', 'datetime'],
            [Types.STRING(), Types.DOUBLE(), Types.STRING()])).build()
    ) \
    .build()

stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "vitesse_moyenne_localisation")

# Premier map : (clé/tronçon_id, vitesse_moyenne, 1)  # (1 pour compter le nombre de vitesse moyenne)
# Key by : tronçon_id
# Window : Affichage toutes les 20 secondes
# Reduce : (tronçon_id, somme_des_vitesses_moyennes, somme_des_nombres_de_vitesse_moyenne)
# Deuxième map : (tronçon_id, moyenne_des_vitesses_moyennes, nombre_de_vitesse_moyenne)
# Print : (clé/tronçon_id, moyenne_des_vitesses_moyennes, nombre_de_vitesse_moyenne)
stream .map(lambda ligne: (ligne[0], ligne[1], 1), output_type=Types.TUPLE([Types.STRING(), Types.DOUBLE(), Types.INT()])) \
    .key_by(lambda ligne: ligne[0]) \
    .window(TumblingProcessingTimeWindows.of(Time.seconds(20))) \
    .reduce(lambda a, b: (a[0], a[1]+b[1], a[2]+b[2])) \
    .map(lambda result: (result[0], round(result[1]/result[2], 1), result[2])) \
    .print()

env.execute()
