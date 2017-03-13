# Kukua Stats

> Kukua statistics.

## Setup

```bash
git clone https://github.com/kukua/kukua-stats.git
cd kukua-stats/
cp .env.example .env
chmod 600 .env
# > Edit .env
docker-compose run --rm generate npm install

# Manually
docker-compose up generate

# Cronjob
sudo cp ./cronjob /etc/cron.d/kukua-stats
# > Edit path in /etc/cron.d/kukua-stats
sudo service cron reload
```

## License

This software is licensed under the [MIT license](https://github.com/kukua/kukua-stats/blob/master/LICENSE).

© 2017 Kukua BV
