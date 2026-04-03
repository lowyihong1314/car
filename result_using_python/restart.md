# 1. 处理 3983 行 unknown（大部分有 patterns 瞬间判断，无需爬网）
python classify_with_crawler.py --browser --limit 5000 --delay 1.5 --allow-unknown

# 2. 处理 223 行空白 None
python classify_with_crawler.py --browser --limit 500 --delay 1.5

# 3. 修正 1528 行误判的 electric/petrol（无 hybrid 关键词的）
python classify_with_crawler.py --browser --limit 2000 --delay 1.5 --fix-hybrid
