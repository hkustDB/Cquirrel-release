import DataGenerator
import unittest
import os
import shutil
import configparser
import subprocess
import filecmp

from confluent_kafka import Consumer

class TestDataGenerator(unittest.TestCase):
    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass

    def test_config_file_not_exists(self):
        fake_config_file_name = "fake_config.ini"
        if os.path.exists(fake_config_file_name):
            os.remove(fake_config_file_name)
        self.assertRaises(FileNotFoundError, DataGenerator.data_generator, fake_config_file_name)

    def test_config_file_para_not_match(self):
        fake_config_file_content = """
        [DEFAULT]
        outputname = asdf.csv
        """
        fake_config_file_name = "fake_config.ini"
        with open(fake_config_file_name, 'w') as f:
            f.write(fake_config_file_content)
        self.assertRaises(Exception, DataGenerator.data_generator, fake_config_file_name)
        if os.path.exists(fake_config_file_name):
            os.remove(fake_config_file_name)

    def test_config_para_data_file_path_not_exists(self):
        fake_config_file_content = """
        [DEFAULT]
        DataFilePath = /fake_config_file_path
        WindowSize = 10
        ScaleFactor = 0.1
        isLineitem = yes
        isOrders = Yes
        isCustomer = Yes
        isPartSupp = Yes
        isPart = Yes
        isSupplier = Yes
        isNation = Yes
        isRegion = yes
        OutputFileName = output_data.csv
        """
        fake_config_file_name = "fake_config.ini"
        with open(fake_config_file_name, 'w') as f:
            f.write(fake_config_file_content)
        self.assertRaises(Exception, DataGenerator.data_generator, fake_config_file_name)
        if os.path.exists(fake_config_file_name):
            os.remove(fake_config_file_name)

    def test_config_para_window_size_is_negative(self):
        fake_config_file_content = """
        [DEFAULT]
        DataFilePath = .
        WindowSize = -10
        ScaleFactor = 0.1
        isLineitem = yes
        isOrders = Yes
        isCustomer = Yes
        isPartSupp = Yes
        isPart = Yes
        isSupplier = Yes
        isNation = Yes
        isRegion = yes
        OutputFileName = output_data.csv
        """
        fake_config_file_name = "fake_config.ini"
        with open(fake_config_file_name, 'w') as f:
            f.write(fake_config_file_content)
        self.assertRaises(ValueError, DataGenerator.data_generator, fake_config_file_name)
        if os.path.exists(fake_config_file_name):
            os.remove(fake_config_file_name)

    def test_config_para_scale_factor_is_negative(self):
        fake_config_file_content = """
        [DEFAULT]
        DataFilePath = .
        WindowSize = 10
        ScaleFactor = -0.1
        isLineitem = yes
        isOrders = Yes
        isCustomer = Yes
        isPartSupp = Yes
        isPart = Yes
        isSupplier = Yes
        isNation = Yes
        isRegion = yes
        OutputFileName = output_data.csv
        """
        fake_config_file_name = "fake_config.ini"
        with open(fake_config_file_name, 'w') as f:
            f.write(fake_config_file_content)
        self.assertRaises(ValueError, DataGenerator.data_generator, fake_config_file_name)
        if os.path.exists(fake_config_file_name):
            os.remove(fake_config_file_name)

    def test_config_para_scale_factor_is_zero(self):
        fake_config_file_content = """
        [DEFAULT]
        DataFilePath = .
        WindowSize = 10
        ScaleFactor = 0
        isLineitem = yes
        isOrders = Yes
        isCustomer = Yes
        isPartSupp = Yes
        isPart = Yes
        isSupplier = Yes
        isNation = Yes
        isRegion = yes
        OutputFileName = output_data.csv
        """
        fake_config_file_name = "fake_config.ini"
        with open(fake_config_file_name, 'w') as f:
            f.write(fake_config_file_content)
        self.assertRaises(ValueError, DataGenerator.data_generator, fake_config_file_name)
        if os.path.exists(fake_config_file_name):
            os.remove(fake_config_file_name)

    def test_data_files_not_exists(self):
        fake_config_file_content = """
        [DEFAULT]
        DataFilePath = ./fake_data_file_path
        WindowSize = 10
        ScaleFactor = 0.1
        isLineitem = yes
        isOrders = Yes
        isCustomer = Yes
        isPartSupp = Yes
        isPart = Yes
        isSupplier = Yes
        isNation = Yes
        isRegion = yes
        OutputFileName = output_data.csv
        [KAFKA_CONF]
        KafkaEnable = no
        BootstrapServer = localhost:9092
        KafkaTopic = aju_data_generator
        """
        fake_config_file_name = "fake_config.ini"
        with open(fake_config_file_name, 'w') as f:
            f.write(fake_config_file_content)
        fake_data_file_path = "./fake_data_file_path"
        if os.path.exists(fake_data_file_path):
            shutil.rmtree(fake_data_file_path)
        os.mkdir(fake_data_file_path)

        self.assertRaises(FileNotFoundError, DataGenerator.data_generator, fake_config_file_name)

        if os.path.exists(fake_config_file_name):
            os.remove(fake_config_file_name)
        if os.path.exists(fake_data_file_path):
            shutil.rmtree(fake_data_file_path)

    def test_data_files_not_fully_exists(self):
        fake_config_file_content = """
        [DEFAULT]
        DataFilePath = ./fake_data_file_path
        WindowSize = 10
        ScaleFactor = 0.1
        isLineitem = yes
        isOrders = Yes
        isCustomer = Yes
        isPartSupp = Yes
        isPart = Yes
        isSupplier = Yes
        isNation = Yes
        isRegion = yes
        OutputFileName = output_data.csv
        [KAFKA_CONF]
        KafkaEnable = no
        BootstrapServer = localhost:9092
        KafkaTopic = aju_data_generator
        """
        fake_config_file_name = "fake_config.ini"
        with open(fake_config_file_name, 'w') as f:
            f.write(fake_config_file_content)
        fake_data_file_path = "./fake_data_file_path"
        if os.path.exists(fake_data_file_path):
            shutil.rmtree(fake_data_file_path)
        os.mkdir(fake_data_file_path)
        lineitem_file_path = os.path.join(fake_data_file_path, "lineitem.tbl")
        open(lineitem_file_path, 'w').close()

        self.assertRaises(FileNotFoundError, DataGenerator.data_generator, fake_config_file_name)

        if os.path.exists(fake_config_file_name):
            os.remove(fake_config_file_name)
        if os.path.exists(fake_data_file_path):
            shutil.rmtree(fake_data_file_path)

    def test_data_files_are_empty(self):
        fake_config_file_content = """
        [DEFAULT]
        DataFilePath = ./fake_data_file_path
        WindowSize = 10
        ScaleFactor = 0.1
        isLineitem = yes
        isOrders = Yes
        isCustomer = Yes
        isPartSupp = Yes
        isPart = Yes
        isSupplier = Yes
        isNation = Yes
        isRegion = yes
        OutputFileName = output_data.csv
        """
        fake_config_file_name = "fake_config.ini"
        with open(fake_config_file_name, 'w') as f:
            f.write(fake_config_file_content)
        fake_data_file_path = "./fake_data_file_path"
        if os.path.exists(fake_data_file_path):
            shutil.rmtree(fake_data_file_path)
        os.mkdir(fake_data_file_path)
        customer_file_path = os.path.join(fake_data_file_path, "customer.tbl")
        lineitem_file_path = os.path.join(fake_data_file_path, "lineitem.tbl")
        nation_file_path = os.path.join(fake_data_file_path, "nation.tbl")
        orders_file_path = os.path.join(fake_data_file_path, "orders.tbl")
        part_file_path = os.path.join(fake_data_file_path, "part.tbl")
        partsupp_file_path = os.path.join(fake_data_file_path, "partsupp.tbl")
        region_file_path = os.path.join(fake_data_file_path, "region.tbl")
        supplier_file_path = os.path.join(fake_data_file_path, "supplier.tbl")
        open(customer_file_path, 'w').close()
        open(lineitem_file_path, 'w').close()
        open(nation_file_path, 'w').close()
        open(orders_file_path, 'w').close()
        open(part_file_path, 'w').close()
        open(partsupp_file_path, 'w').close()
        open(region_file_path, 'w').close()
        open(supplier_file_path, 'w').close()

        self.assertRaises(Exception, DataGenerator.data_generator, fake_config_file_name)

        if os.path.exists(fake_config_file_name):
            os.remove(fake_config_file_name)
        if os.path.exists(fake_data_file_path):
            shutil.rmtree(fake_data_file_path)

    def test_data_files_are_not_enough(self):
        fake_config_file_content = """
        [DEFAULT]
        DataFilePath = ./fake_data_file_path
        WindowSize = 10
        ScaleFactor = 0.1
        isLineitem = yes
        isOrders = Yes
        isCustomer = Yes
        isPartSupp = Yes
        isPart = Yes
        isSupplier = Yes
        isNation = Yes
        isRegion = yes
        OutputFileName = output_data.csv
        """
        fake_config_file_name = "fake_config.ini"
        with open(fake_config_file_name, 'w') as f:
            f.write(fake_config_file_content)
        fake_data_file_path = "./fake_data_file_path"
        if os.path.exists(fake_data_file_path):
            shutil.rmtree(fake_data_file_path)

        os.mkdir(fake_data_file_path)
        customer_file_path = os.path.join(fake_data_file_path, "customer.tbl")
        lineitem_file_path = os.path.join(fake_data_file_path, "lineitem.tbl")
        nation_file_path = os.path.join(fake_data_file_path, "nation.tbl")
        orders_file_path = os.path.join(fake_data_file_path, "orders.tbl")
        part_file_path = os.path.join(fake_data_file_path, "part.tbl")
        partsupp_file_path = os.path.join(fake_data_file_path, "partsupp.tbl")
        region_file_path = os.path.join(fake_data_file_path, "region.tbl")
        supplier_file_path = os.path.join(fake_data_file_path, "supplier.tbl")

        with open(customer_file_path, 'w') as f:
            f.write("abc")
        with open(customer_file_path, 'w') as f:
            f.write("abc")
        with open(lineitem_file_path, 'w') as f:
            f.write("abc")
        with open(nation_file_path, 'w') as f:
            f.write("abc")
        with open(orders_file_path, 'w') as f:
            f.write("abc")
        with open(part_file_path, 'w') as f:
            f.write("abc")
        with open(partsupp_file_path, 'w') as f:
            f.write("abc")
        with open(region_file_path, 'w') as f:
            f.write("abc")
        with open(supplier_file_path, 'w') as f:
            f.write("abc")

        self.assertRaises(Exception, DataGenerator.data_generator, fake_config_file_name)

        if os.path.exists(fake_config_file_name):
            os.remove(fake_config_file_name)
        if os.path.exists(fake_data_file_path):
            shutil.rmtree(fake_data_file_path)

    def test_output_file_line_match(self):
        DataGenerator.data_generator('test_config.ini')
        config = configparser.ConfigParser()
        config.read('test_config.ini')
        customer_path = os.path.join(config['DEFAULT']['DataFilePath'], 'customer.tbl')
        lineitem_path = os.path.join(config['DEFAULT']['DataFilePath'], 'lineitem.tbl')
        nation_path = os.path.join(config['DEFAULT']['DataFilePath'], 'nation.tbl')
        orders_path = os.path.join(config['DEFAULT']['DataFilePath'], 'orders.tbl')
        part_path = os.path.join(config['DEFAULT']['DataFilePath'], 'part.tbl')
        partsupp_path = os.path.join(config['DEFAULT']['DataFilePath'], 'partsupp.tbl')
        region_path = os.path.join(config['DEFAULT']['DataFilePath'], 'region.tbl')
        supplier_path = os.path.join(config['DEFAULT']['DataFilePath'], 'supplier.tbl')
        output_path = os.path.join(config['DEFAULT']['DataFilePath'], config['DEFAULT']['OutputFileName'])

        with open(customer_path, 'r') as f:
            customer_line_num = len(f.readlines())
        with open(lineitem_path, 'r') as f:
            lineitem_line_num = len(f.readlines())
        with open(nation_path, 'r') as f:
            nation_line_num = len(f.readlines())
        with open(orders_path, 'r') as f:
            orders_line_num = len(f.readlines())
        with open(part_path, 'r') as f:
            part_line_num = len(f.readlines())
        with open(partsupp_path, 'r') as f:
            partsupp_line_num = len(f.readlines())
        with open(region_path, 'r') as f:
            region_line_num = len(f.readlines())
        with open(supplier_path, 'r') as f:
            supplier_line_num = len(f.readlines())
        with open(output_path, 'r') as f:
            output_line_num = len(f.readlines())

        self.assertEqual(output_line_num / 2,
                         customer_line_num + lineitem_line_num + nation_line_num + orders_line_num + part_line_num +
                         partsupp_line_num + region_line_num + supplier_line_num)

    def test_output_file_separate_match(self):
        DataGenerator.data_generator('test_config.ini')
        config = configparser.ConfigParser()
        config.read('test_config.ini')
        customer_path = os.path.join(config['DEFAULT']['DataFilePath'], 'customer.tbl')
        lineitem_path = os.path.join(config['DEFAULT']['DataFilePath'], 'lineitem.tbl')
        nation_path = os.path.join(config['DEFAULT']['DataFilePath'], 'nation.tbl')
        orders_path = os.path.join(config['DEFAULT']['DataFilePath'], 'orders.tbl')
        part_path = os.path.join(config['DEFAULT']['DataFilePath'], 'part.tbl')
        partsupp_path = os.path.join(config['DEFAULT']['DataFilePath'], 'partsupp.tbl')
        region_path = os.path.join(config['DEFAULT']['DataFilePath'], 'region.tbl')
        supplier_path = os.path.join(config['DEFAULT']['DataFilePath'], 'supplier.tbl')
        output_path = os.path.join(config['DEFAULT']['DataFilePath'], config['DEFAULT']['OutputFileName'])

        with open(customer_path, 'r') as f:
            customer_line_num = len(f.readlines())
        with open(lineitem_path, 'r') as f:
            lineitem_line_num = len(f.readlines())
        with open(nation_path, 'r') as f:
            nation_line_num = len(f.readlines())
        with open(orders_path, 'r') as f:
            orders_line_num = len(f.readlines())
        with open(part_path, 'r') as f:
            part_line_num = len(f.readlines())
        with open(partsupp_path, 'r') as f:
            partsupp_line_num = len(f.readlines())
        with open(region_path, 'r') as f:
            region_line_num = len(f.readlines())
        with open(supplier_path, 'r') as f:
            supplier_line_num = len(f.readlines())
        with open(output_path, 'r') as f:
            output_separate_count = 0
            while True:
                line = f.readline()
                output_separate_count = output_separate_count + line.count('|')
                if not line:
                    break

        self.assertEqual(output_separate_count / 2,
                         customer_line_num * 8 +
                         lineitem_line_num * 16 +
                         nation_line_num * 4 +
                         orders_line_num * 9 +
                         part_line_num * 9 +
                         partsupp_line_num * 5 +
                         region_line_num * 3 +
                         supplier_line_num * 7)

    def test_kafka_config_section_not_exist(self):
        fake_config_file_content = """
        [DEFAULT]
        DataFilePath = ./fake_data_file_path
        WindowSize = 10
        ScaleFactor = 0.1
        isLineitem = yes
        isOrders = Yes
        isCustomer = Yes
        isPartSupp = Yes
        isPart = Yes
        isSupplier = Yes
        isNation = Yes
        isRegion = yes
        OutputFileName = output_data.csv
        """
        fake_config_file_name = "fake_config.ini"
        with open(fake_config_file_name, 'w') as f:
            f.write(fake_config_file_content)

        self.assertRaises(Exception, DataGenerator.data_generator, fake_config_file_name)

        if os.path.exists(fake_config_file_name):
            os.remove(fake_config_file_name)

    def test_kafka_config_enable_not_exist(self):
        fake_config_file_content = """
        [DEFAULT]
        DataFilePath = ./fake_data_file_path
        WindowSize = 10
        ScaleFactor = 0.1
        isLineitem = yes
        isOrders = Yes
        isCustomer = Yes
        isPartSupp = Yes
        isPart = Yes
        isSupplier = Yes
        isNation = Yes
        isRegion = yes
        OutputFileName = output_data.csv
        [KAFKA_CONF]
        """
        fake_config_file_name = "fake_config.ini"
        with open(fake_config_file_name, 'w') as f:
            f.write(fake_config_file_content)

        self.assertRaises(Exception, DataGenerator.data_generator, fake_config_file_name)

        if os.path.exists(fake_config_file_name):
            os.remove(fake_config_file_name)

    def test_kafka_config_bootstrapserver_not_exist(self):
        fake_config_file_content = """
        [DEFAULT]
        DataFilePath = ./fake_data_file_path
        WindowSize = 10
        ScaleFactor = 0.1
        isLineitem = yes
        isOrders = Yes
        isCustomer = Yes
        isPartSupp = Yes
        isPart = Yes
        isSupplier = Yes
        isNation = Yes
        isRegion = yes
        OutputFileName = output_data.csv
        [KAFKA_CONF]
        KafkaEnable=no
        """
        fake_config_file_name = "fake_config.ini"
        with open(fake_config_file_name, 'w') as f:
            f.write(fake_config_file_content)

        self.assertRaises(Exception, DataGenerator.data_generator, fake_config_file_name)

        if os.path.exists(fake_config_file_name):
            os.remove(fake_config_file_name)

    def test_kafka_config_kafkatopic_not_exist(self):
        fake_config_file_content = """
        [DEFAULT]
        DataFilePath = ./fake_data_file_path
        WindowSize = 10
        ScaleFactor = 0.1
        isLineitem = yes
        isOrders = Yes
        isCustomer = Yes
        isPartSupp = Yes
        isPart = Yes
        isSupplier = Yes
        isNation = Yes
        isRegion = yes
        OutputFileName = output_data.csv
        [KAFKA_CONF]
        KafkaEnable=no
        BootstrapServer = localhost:9092
        """
        fake_config_file_name = "fake_config.ini"
        with open(fake_config_file_name, 'w') as f:
            f.write(fake_config_file_content)

        self.assertRaises(Exception, DataGenerator.data_generator, fake_config_file_name)

        if os.path.exists(fake_config_file_name):
            os.remove(fake_config_file_name)

    def test_kafka_output_lines_match(self):
        test_config_file_name = 'test_config_kafka.ini'

        # delete and create kafka topic
        KAFKA_HOME_PATH = '/Users/chaoqi/Programs/kafka_2.12-2.6.0'
        delete_topic_cmd = KAFKA_HOME_PATH + '/' +'bin/kafka-topics.sh --delete -topic test_aju_data_generator --zookeeper localhost:2181'
        create_topic_cmd = KAFKA_HOME_PATH + '/' + 'bin/kafka-topics.sh --create -topic test_aju_data_generator --bootstrap-server localhost:9092'
        subprocess.run(delete_topic_cmd, shell=True)
        subprocess.run(create_topic_cmd, shell=True)

        DataGenerator.data_generator(test_config_file_name)

        test_output_file_name = 'test_output_data.csv'
        kafka_output_file_name = 'kafka_output_data.csv'

        # create kafka consumer
        kafka_consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'test_aju_dg_group',
            'auto.offset.reset': 'earliest'
        })
        kafka_consumer.subscribe(['test_aju_data_generator'])
        with open(kafka_output_file_name, 'w') as f:
            kafka_consumer_count = 0
            while True:
                msg = kafka_consumer.poll(1)
                if msg is None:
                    break
                if msg.error():
                    print("Consumer error: {}".format(msg.error()))
                    break
                f.write(msg.value().decode('utf-8'))
                kafka_consumer_count = kafka_consumer_count + 1
                if kafka_consumer_count % 100000 == 0:
                    print('kafka consumer consume: ' + str(kafka_consumer_count))
            print('kafka consumer consume: ' + str(kafka_consumer_count))
        kafka_consumer.close()

        self.assertTrue(filecmp.cmp(test_output_file_name, kafka_output_file_name))

        if os.path.exists(test_output_file_name):
            os.remove(test_output_file_name)
        if os.path.exists(kafka_output_file_name):
            os.remove(kafka_output_file_name)


if __name__ == '__main__':
    unittest.main()
