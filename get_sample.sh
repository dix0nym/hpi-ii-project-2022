for i in {0..15}
do
	poetry run python rb_crawler/main.py --id $((i * 50000)) --state by --stop_id $((i * 50000 + 125))
	poetry run python rb_crawler/main.py --id $((i * 50000)) --state bw --stop_id $((i * 50000 + 125))
	poetry run python rb_crawler/main.py --id $((i * 50000)) --state be --stop_id $((i * 50000 + 125))
	poetry run python rb_crawler/main.py --id $((i * 50000)) --state br --stop_id $((i * 50000 + 125))
	poetry run python rb_crawler/main.py --id $((i * 50000)) --state hb --stop_id $((i * 50000 + 125))
	poetry run python rb_crawler/main.py --id $((i * 50000)) --state hh --stop_id $((i * 50000 + 125))
	poetry run python rb_crawler/main.py --id $((i * 50000)) --state he --stop_id $((i * 50000 + 125))
	poetry run python rb_crawler/main.py --id $((i * 50000)) --state mv --stop_id $((i * 50000 + 125))
	poetry run python rb_crawler/main.py --id $((i * 50000)) --state ni --stop_id $((i * 50000 + 125))
	poetry run python rb_crawler/main.py --id $((i * 50000)) --state nw --stop_id $((i * 50000 + 125))
	poetry run python rb_crawler/main.py --id $((i * 50000)) --state rp --stop_id $((i * 50000 + 125))
	poetry run python rb_crawler/main.py --id $((i * 50000)) --state sl --stop_id $((i * 50000 + 125))
	poetry run python rb_crawler/main.py --id $((i * 50000)) --state sn --stop_id $((i * 50000 + 125))
	poetry run python rb_crawler/main.py --id $((i * 50000)) --state st --stop_id $((i * 50000 + 125))
	poetry run python rb_crawler/main.py --id $((i * 50000)) --state sh --stop_id $((i * 50000 + 125))
	poetry run python rb_crawler/main.py --id $((i * 50000)) --state th --stop_id $((i * 50000 + 125))
done

for i in {15..50}
do
	poetry run python rb_crawler/main.py --id $((i * 50000)) --state nw --stop_id $((i * 50000 + 125))
done
