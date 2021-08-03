import os
import configparser

from confluent_kafka import Producer

CONFIG_PARA = ['datafilepath', 'windowsize', 'scalefactor', 'islineitem', 'isorders', 'iscustomer', 'ispartsupp',
               'ispart', 'issupplier', 'isnation', 'isregion' 'outputfilename']


def data_generator(config_file_name):
    # read the settings from file
    conf = configparser.ConfigParser()
    if not os.path.exists(config_file_name):
        raise FileNotFoundError('config file not found.')
    conf.read(config_file_name, encoding='UTF-8')
    # select the section DEFAULT
    config = conf['DEFAULT']
    config_list = [x for x in config]
    # check if the key of config are fully correct
    if config_list.sort() != CONFIG_PARA.sort():
        raise Exception("config parameters should contain 'datafilepath', 'windowsize', 'scalefactor', 'islineitem', "
                        "'isorders', 'iscustomer', 'ispartsupp', 'ispart', 'issupplier', 'isnation', 'outputfilename'.")

    data_file_path_prefix = config['DataFilePath']
    if not os.path.exists(data_file_path_prefix):
        raise Exception("data file path does not exists.")
    window_size = config.getint('WindowSize')
    if window_size < 0:
        raise ValueError("window size should not be negative.")
    scale_factor = config.getfloat('ScaleFactor')
    if scale_factor <= 0:
        raise ValueError("scale factor should be positive.")

    isLineitem = config.getboolean('isLineitem')
    isOrders = config.getboolean('isOrders')
    isCustomer = config.getboolean('isCustomer')
    isPartSupp = config.getboolean('isPartSupp')
    isPart = config.getboolean('isPart')
    isSupplier = config.getboolean('isSupplier')
    isNation = config.getboolean('isNation')
    isRegion = config.getboolean('isRegion')

    # set the output file
    output_file_path = os.path.join(data_file_path_prefix, config['OutputFileName'])
    output = open(output_file_path, "w")

    # select the section KAFKA_CONF
    if 'KAFKA_CONF' not in conf.sections():
        raise Exception('KAFKA_CONF section should be in the config file.')
    config = conf['KAFKA_CONF']
    if 'KafkaEnable' not in config:
        raise Exception('KafkaEnable should be in the config file.')
    kafka_enable = config.getboolean('KafkaEnable')
    if 'BootstrapServer' not in config:
        raise Exception('BootstrapServer should be in the config file.')
    kafka_bootstrap_server = config['BootstrapServer']
    if 'KafkaTopic' not in config:
        raise Exception('KafkaTopic should be in the config file.')
    kafka_topic = config['KafkaTopic']
    # kafka_partition = config.getint('KafkaPartition')
    if kafka_enable:
        kafka_producer = Producer({'bootstrap.servers': kafka_bootstrap_server})

    # define a output func to write data to file and kafka
    def write_to_output_and_kafka(s):
        output.write(s)
        if kafka_enable:
            try:
                kafka_producer.produce(kafka_topic, s.encode('utf-8'))
                kafka_producer.poll(0)
            except BufferError:
                print('Buffer error, the queue must be full! Flushing...')
                kafka_producer.flush()
                print('Queue flushed, will write the message again')
                kafka_producer.produce(kafka_topic, s.encode('utf-8'))
                kafka_producer.poll(0)

    # set the size of different tables
    lineitem_size = scale_factor * 6000000
    orders_size = scale_factor * 1500000
    nation_size = 25
    region_size = 5
    partsupp_size = scale_factor * 800000
    part_size = scale_factor * 200000
    supplier_size = scale_factor * 10000
    customer_size = scale_factor * 150000

    tpch_tables = ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']
    tpch_tables_lines_num = [customer_size, lineitem_size, nation_size, orders_size, part_size, partsupp_size,
                             region_size, supplier_size]
    tpch_tables_path = [os.path.join(data_file_path_prefix, x + ".tbl") for x in tpch_tables]

    # check if the files exists or not
    for ttp in tpch_tables_path:
        if not os.path.exists(ttp):
            raise FileNotFoundError("{} not found.".format(ttp))
    # check if the files are null
    for ttp in tpch_tables_path:
        if not os.path.getsize(ttp):
            raise Exception("{} is an empty file.".format(ttp))
    # check the lines number of files
    print("*************************************")
    for index in range(len(tpch_tables_path)):
        with open(tpch_tables_path[index], 'r') as f:
            lines_num = len(f.readlines())
        if lines_num < tpch_tables_lines_num[index]:
            raise Exception(
                "{} 's lines number ({}) is not enough.".format(tpch_tables_path[index], tpch_tables_lines_num[index]))
        else:
            print("{} has {} lines".format(tpch_tables_path[index], lines_num))

    # open the files
    lineitem = open(os.path.join(data_file_path_prefix, "lineitem.tbl"), "r")
    orders = open(os.path.join(data_file_path_prefix, "orders.tbl"), "r")
    partsupp = open(os.path.join(data_file_path_prefix, "partsupp.tbl"), "r")
    part = open(os.path.join(data_file_path_prefix, "part.tbl"), "r")
    supplier = open(os.path.join(data_file_path_prefix, "supplier.tbl"), "r")
    customer = open(os.path.join(data_file_path_prefix, "customer.tbl"), "r")
    nation = open(os.path.join(data_file_path_prefix, "nation.tbl"), "r")
    region = open(os.path.join(data_file_path_prefix, "region.tbl"), "r")

    # delete = (window_size != 0)
    lineitem_d = open(os.path.join(data_file_path_prefix, "lineitem.tbl"), "r")
    orders_d = open(os.path.join(data_file_path_prefix, "orders.tbl"), "r")
    partsupp_d = open(os.path.join(data_file_path_prefix, "partsupp.tbl"), "r")
    part_d = open(os.path.join(data_file_path_prefix, "part.tbl"), "r")
    supplier_d = open(os.path.join(data_file_path_prefix, "supplier.tbl"), "r")
    customer_d = open(os.path.join(data_file_path_prefix, "customer.tbl"), "r")
    nation_d = open(os.path.join(data_file_path_prefix, "nation.tbl"), "r")
    region_d = open(os.path.join(data_file_path_prefix, "region.tbl"), "r")

    # init the count number
    count = 0
    delete_count = 0 - window_size
    part_count = 0
    part_delete_count = 0
    supplier_count = 0
    supplier_delete_count = 0
    orders_count = 0
    orders_delete_count = 0
    customer_count = 0
    customer_delete_count = 0
    partsupp_count = 0
    partsupp_delete_count = 0
    nation_count = 0
    nation_delete_count = 0
    region_count = 0
    region_delete_count = 0

    # read the first line
    line_lineitem = lineitem.readline()
    line_lineitem_d = lineitem_d.readline()
    line_orders = orders.readline()
    line_orders_d = orders_d.readline()
    line_partsupp = partsupp.readline()
    line_partsupp_d = partsupp_d.readline()
    line_part = part.readline()
    line_part_d = part_d.readline()
    line_supplier = supplier.readline()
    line_supplier_d = supplier_d.readline()
    line_customer = customer.readline()
    line_customer_d = customer_d.readline()
    line_nation = nation.readline()
    line_nation_d = nation_d.readline()
    line_region = region.readline()
    line_region_d = region_d.readline()

    # write the first part
    while line_lineitem:
        count = count + 1
        delete_count = delete_count + 1
        if isLineitem:
            write_to_output_and_kafka("+LI" + line_lineitem)
        line_lineitem = lineitem.readline()
        if delete_count > 0:
            if isLineitem:
                write_to_output_and_kafka("-LI" + line_lineitem_d)
            line_lineitem_d = lineitem_d.readline()

        if count * orders_size / lineitem_size > orders_count and line_orders:
            orders_count = orders_count + 1
            if isOrders:
                write_to_output_and_kafka("+OR" + line_orders)
            line_orders = orders.readline()
            if delete_count * orders_size / lineitem_size > orders_delete_count and line_orders_d:
                orders_delete_count = orders_delete_count + 1
                if isOrders:
                    write_to_output_and_kafka("-OR" + line_orders_d)
                line_orders_d = orders_d.readline()

        if count * customer_size / lineitem_size > customer_count and line_customer:
            customer_count = customer_count + 1
            if isCustomer:
                write_to_output_and_kafka("+CU" + line_customer)
            line_customer = customer.readline()
            if delete_count * customer_size / lineitem_size > customer_delete_count and line_customer_d:
                customer_delete_count = customer_delete_count + 1
                if isCustomer:
                    write_to_output_and_kafka("-CU" + line_customer_d)
                line_customer_d = customer_d.readline()

        if count * part_size / lineitem_size > part_count and line_part:
            part_count = part_count + 1
            if isPart:
                write_to_output_and_kafka("+PA" + line_part)
            line_part = part.readline()
            if delete_count * part_size / lineitem_size > part_delete_count and line_part_d:
                part_delete_count = part_delete_count + 1
                if isPart:
                    write_to_output_and_kafka("-PA" + line_part_d)
                line_part_d = part_d.readline()

        if count * supplier_size / lineitem_size > supplier_count and line_supplier:
            supplier_count = supplier_count + 1
            if isSupplier:
                write_to_output_and_kafka("+SU" + line_supplier)
            line_supplier = supplier.readline()
            if delete_count * supplier_size / lineitem_size > supplier_delete_count and line_supplier_d:
                supplier_delete_count = supplier_delete_count + 1
                if isSupplier:
                    write_to_output_and_kafka("-SU" + line_supplier_d)
                line_supplier_d = supplier_d.readline()

        if count * partsupp_size / lineitem_size > partsupp_count and line_partsupp:
            partsupp_count = partsupp_count + 1
            if isPartSupp:
                write_to_output_and_kafka("+PS" + line_partsupp)
            line_partsupp = partsupp.readline()
            if delete_count * partsupp_size / lineitem_size > partsupp_delete_count and line_partsupp_d:
                partsupp_delete_count = partsupp_delete_count + 1
                if isPartSupp:
                    write_to_output_and_kafka("-PS" + line_partsupp_d)
                line_partsupp_d = partsupp_d.readline()

        if count * nation_size / lineitem_size > nation_count and line_nation:
            nation_count = nation_count + 1
            if isNation:
                write_to_output_and_kafka("+NA" + line_nation)
            line_nation = nation.readline()
            if delete_count * nation_size / lineitem_size > nation_delete_count and line_nation_d:
                nation_delete_count = nation_delete_count + 1
                if isNation:
                    write_to_output_and_kafka("-NA" + line_nation_d)
                line_nation_d = nation_d.readline()

        if count * region_size / lineitem_size > region_count and line_region:
            region_count = region_count + 1
            if isRegion:
                write_to_output_and_kafka("+RI" + line_region)
            line_region = region.readline()
            if delete_count * region_size / lineitem_size > region_delete_count and line_region_d:
                region_delete_count = region_delete_count + 1
                if isRegion:
                    write_to_output_and_kafka("-RI" + line_region_d)
                line_region_d = region_d.readline()

    # write the second part
    while line_lineitem_d:
        delete_count = delete_count + 1
        if isLineitem:
            write_to_output_and_kafka("-LI" + line_lineitem_d)
        line_lineitem_d = lineitem_d.readline()

        if delete_count * orders_size / lineitem_size > orders_delete_count and line_orders_d:
            orders_delete_count = orders_delete_count + 1
            if isOrders:
                write_to_output_and_kafka("-OR" + line_orders_d)
            line_orders_d = orders_d.readline()

        if delete_count * customer_size / lineitem_size > customer_delete_count and line_customer_d:
            customer_delete_count = customer_delete_count + 1
            if isCustomer:
                write_to_output_and_kafka("-CU" + line_customer_d)
            line_customer_d = customer_d.readline()

        if delete_count * part_size / lineitem_size > part_delete_count and line_part_d:
            part_delete_count = part_delete_count + 1
            if isPart:
                write_to_output_and_kafka("-PA" + line_part_d)
            line_part_d = part_d.readline()

        if delete_count * supplier_size / lineitem_size > supplier_delete_count and line_supplier_d:
            supplier_delete_count = supplier_delete_count + 1
            if isSupplier:
                write_to_output_and_kafka("-SU" + line_supplier_d)
            line_supplier_d = supplier_d.readline()

        if delete_count * partsupp_size / lineitem_size > partsupp_delete_count and line_partsupp_d:
            partsupp_delete_count = partsupp_delete_count + 1
            if isPartSupp:
                write_to_output_and_kafka("-PS" + line_partsupp_d)
            line_partsupp_d = partsupp_d.readline()

        if delete_count * nation_size / lineitem_size > nation_delete_count and line_nation_d:
            nation_delete_count = nation_delete_count + 1
            if isNation:
                write_to_output_and_kafka("-NA" + line_nation_d)
            line_nation_d = nation_d.readline()

        if delete_count * region_size / lineitem_size > region_delete_count and line_region_d:
            region_delete_count = region_delete_count + 1
            if isRegion:
                write_to_output_and_kafka("-RI" + line_region_d)
            line_region_d = region_d.readline()

    # close the file
    lineitem.close()
    orders.close()
    partsupp.close()
    part.close()
    supplier.close()
    customer.close()
    nation.close()
    region.close()

    lineitem_d.close()
    orders_d.close()
    partsupp_d.close()
    part_d.close()
    supplier_d.close()
    customer_d.close()
    nation_d.close()
    region_d.close()

    output.close()

    print("")
    print("*************************************")
    print("count: ", count)
    print("delete_count: ", delete_count)
    print("customer_count: ", customer_count)
    print("customer_delete_count: ", customer_delete_count)
    print("nation_count: ", nation_count)
    print("nation_delete_count: ", nation_delete_count)
    print("orders_count: ", orders_count)
    print("orders_delete_count: ", orders_delete_count)
    print("part_count: ", part_count)
    print("part_delete_count: ", part_delete_count)
    print("partsupp_count: ", partsupp_count)
    print("partsupp_delete_count: ", partsupp_delete_count)
    print("region_count: ", region_count)
    print("region_delete_count: ", region_delete_count)
    print("supplier_count: ", supplier_count)
    print("supplier_delete_count: ", supplier_delete_count)

    print("\ntotal count: ", count + delete_count + part_count + part_delete_count + orders_count +
          orders_delete_count + partsupp_count + partsupp_delete_count + supplier_count + supplier_delete_count +
          customer_count + customer_delete_count + region_count + region_delete_count + nation_count +
          nation_delete_count)

    with open(output_file_path, 'r') as f:
        res_lines_num = len(f.readlines())
    print("output file lines number: ", res_lines_num)
    print("finished: ", config_file_name)
    print("outputfile: ", output_file_path)


if __name__ == '__main__':
    import time

    start = time.time()
    data_generator('config_all.ini')
    end = time.time()
    print("cpu time: ", end - start)
