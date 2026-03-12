import re
import shutil
import argparse
import jsonlines
from datetime import datetime
from pathlib import Path


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('src')
    parser.add_argument('date')
    args = parser.parse_args()

    src = Path(args.src)
    assert src.exists(), f'Path {src} does not exist.'
    date = args.date
    date_format = '%Y-%m-%d'
    try:
        datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        print('The date must be in the format xxxx-xx-xx')
        raise

    date_pattern = r'\d{4}-\d{2}-\d{2}'
    src_date = re.findall(date_pattern, str(src))[0]
    if datetime.strptime(src_date, date_format) != datetime.strptime(date, date_format):
        for pth in src.glob('*.jsonl'):
            data = []
            with jsonlines.open(pth, 'r') as reader:
                for line in reader:
                    line['date_crawl'] = date
                    data.append(line)

            with jsonlines.open(pth, 'w') as writer:
                writer.write_all(data)
        shutil.move(src, re.sub(date_pattern, date, str(src)))
