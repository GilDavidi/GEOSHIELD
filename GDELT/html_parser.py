from newspaper import Article

def get_article_text(url):
    try:
        article = Article(url, browser_user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3')
        article.download()
        article.parse()
        return article.text
    except Exception as e:
        print("An error occurred:", e)
        return "N/A"

