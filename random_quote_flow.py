from metaflow import FlowSpec, step, card
import random

class RandomQuoteFlow(FlowSpec):

    @step
    def start(self):
        print("Starting RandomQuoteFlow...")
        self.next(self.generate_quote)

    @step
    def generate_quote(self):
        quotes = [
            "Believe you can and you're halfway there.",
            "Success is not final, failure is not fatal: It is the courage to continue that counts.",
            "Don't watch the clock; do what it does. Keep going.",
            "The future depends on what you do today.",
            "Dream big and dare to fail."
        ]
        self.quote = random.choice(quotes)
        print(f"Selected Quote: {self.quote}")
        self.next(self.visualize)

    @card(type='markdown')
    @step
    def visualize(self):
        self.md = f"""
        # Motivational Quote
        **Quote:**  
        {self.quote}
        """
        print("Markdown card created.")
        self.next(self.end)

    @step
    def end(self):
        print("Workflow completed successfully!")

if __name__ == '__main__':
    RandomQuoteFlow()