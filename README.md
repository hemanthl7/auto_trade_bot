
<h3 align="center">webhook-trade-bot</h3>

  <p align="center">
    A simple webhook triggered buy/sell bot running on AWS Flask with python chalice.
    Uses the alpaca-trade-api python lib to push orders.
    Default is paper trade. If you want to do real just change the endpoint.
    Set your maximum balance and the allowance you want for trades inside __init__ 
    Also remember to set a TV_Key to authenticate your tradingview requests.
    <br />
  </p>
</div>


<!-- GETTING STARTED -->

### Prerequisites

Create an alpaca trading account.
Default is paper trade. If you want to do real just change the endpoint inside app.py
Set your maximum balance and the allowance you want for trades inside __init__
Also remember to set a TV_Key to authenticate your tradingview requests.

### Installation

1. Get your API Keys from AWS, 
2. Clone the repo
   ```sh
   git clone https://github.com/softee-p/webhook-trade-bot.git
   ```
3. Install packages
   ```sh
   conda update conda
   conda create -n alert-bot python=3.9
   conda activate alert-bot
   python -m pip install chalice webhook-trade-bot
   ```
4. Enter your AWS API key
   ```sh
   /.aws/config
   ```
   *Windows
   ```sh
   /C:\Users\user\.aws\config
   ```

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- USAGE EXAMPLES -->

*Test
1. cd to project directory
2. start chalice locally
   ```sh
   chalice local
   ```
3. Send requests using Insomnia with the following template to http://localhost:8000/sprtrnd
   ```sh
   {"TVkey": "YourTVKey", "asset_type": "crypto", "action": "sell", "open": 222, "high": 233, "low": 210, "close": 1706, "exchange": "CBSE", "ticker": "ETHUSD", "volume": 11524, "time": "1830", "timenow": "1831"}
   ```
   *Just random numbers for example


*Use
1. upload to aws lambda
   ```sh
   chalice deploy
   ```
2. get the url and append /sprtrnd
3. inside tradingview, paste in the alert webhook notification field.
4. add following template to alert message body
   ```js
   {
    "TVkey": "SomeKeyToAuthenticate",
    "asset_type": "crypto",
    #           OR "stock"
    "action": "buy",
    "open": {{open}},
    "high": {{high}},
    "low": {{low}},
    "close": {{close}},
    "exchange": "{{exchange}}",
    "ticker": "{{ticker}}",
    "volume": {{volume}},
    "time": "{{time}}",
    "timenow": "{{timenow}}"
    }
    ```

python -m venv venv

chalice local --no-autoreload