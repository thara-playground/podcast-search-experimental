from flask import Flask, render_template, request
import redis
import repos

r = redis.Redis('localhost', decode_responses=True)

app = Flask(__name__)


@app.route('/index.html', methods=['GET'])
def index():
    words = list(repos.get_all_words(r))
    chuchked = [words[x:x+10] for x in range(0, len(words), 10)]
    return render_template('index.html', words=chuchked)


@app.route('/index.html', methods=['POST'])
def episodes():
    try:
        word = request.form["selected"]
        episode_urls = repos.get_episodes(r, word.encode('utf-8'))

        if episode_urls:
            with r.pipeline(transaction=False) as p:
                for url in episode_urls:
                    repos.get_episode(p, url)
                episodes = p.execute()
        else:
            episodes = []
    except:
        import traceback
        traceback.print_exc()

    return render_template('index.html', word=word, episodes=episodes)
