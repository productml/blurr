# Offer AI Example


Imagine that we have a game. We want to be able to show personalized offers to the user via an in-app purchase.

TODO: Image

This is a model that continuously learns based on the feedback received as the user interacts with the product and makes a purchase (or not).

To train the model, we need a dataset where we can observe how users behave when presented with different offers and prices. We’re assuming that we don’t have that. We want a contextual bandit approach to continuously personalize the ‘winning treatment’ for each user. More on contextual bandits. [here](http://pavel.surmenok.com/2017/08/26/contextual-bandits-and-reinforcement-learning/)

TODO: Broader article comparing A/B tests vs. contextual bandit

Let's start building the model! Taking a first stab at the features we need:

* country
* facebook_connected
* purchases_prev_week_amount
* games_played_last_session
* win_ratio_last_session

The model then predicts

* offer_type
* offer_price

## Raw Data
