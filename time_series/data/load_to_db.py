import argparse
import logging
import os

# LOAD_QUERY = """mysql -ucoha -pcocohaha --local-infile time_series -e "LOAD DATA LOCAL INFILE '{}'  INTO TABLE daily_performance  FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'" """
LOAD_QUERY = """mysql --login-path=daily_performance --local-infile time_series -e "LOAD DATA LOCAL INFILE '{}'  INTO TABLE daily_performance  FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'" """


def get_args():
    parser = argparse.ArgumentParser(description='Time series parser')
    parser.add_argument('--input_dir', type=str, default='updated', help='input directory path')
    parser.add_argument('--output_dir', type=str, default='updated.parsed', help='output directory path')
    return parser.parse_args()


def update_daily_performance():

    def check_line_valid(line):
        if ',' not in line:
            return False
        return True

    def get_date(date):
        date_values = date.split('/')
        date = str(int(date_values[0]) + 1911) + date_values[1] + date_values[2]
        return date

    def update_file_to_database(output_path):
        cmd = LOAD_QUERY.format(output_path)
        logging.info(cmd)
        os.system(cmd)

    args = get_args()
    logging.basicConfig(level=logging.INFO)
    input_dir_path = args.input_dir
    output_dir_path = args.output_dir

    for input_filename in os.listdir(input_dir_path):
        file_id = input_filename.split('.')[0]
        input_path = os.path.join(input_dir_path, input_filename)
        if os.path.isfile(input_path):
            output_path = os.path.join(output_dir_path, input_filename)
            logging.info('Parsing file {}...'.format(input_path))
            with open(input_path, 'r') as f, open(output_path, 'w') as wf:
                for line in f:
                    if not check_line_valid(line):
                        continue
                    values = line.strip('\n').replace('X', '').split(',')
                    s_id, date, open_p, close_p, high, low, label, volume, ntrans, cost = values
                    line = ','.join([s_id, date, volume, cost, open_p, high, low, close_p, label, ntrans]) + '\n'
                    wf.write(line)
            update_file_to_database(output_path)
    logging.info('Done')


def main():
    # load input time series data, then output and update to database
    update_daily_performance()


if __name__ == '__main__':
    main()
