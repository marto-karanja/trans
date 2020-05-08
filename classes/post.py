from wordpress_xmlrpc import Client, WordPressPost
from wordpress_xmlrpc.methods.posts import GetPosts, NewPost

class WpPost():
    """class that posts to wordpress"""
    def __init__(self, url, user, password):
        """initialize class"""
        self.url = url
        self.password = password
        self.user = user
        self.site = Client(url, user, password)

    def publish(self, title, content, categories):
        """publishes post"""
        post = WordPressPost()
        post.title = title
        post.content = content
        post.terms_names = {
            'category': categories
        }
        post.post_status = 'publish'
        self.site.call(NewPost(post))
        