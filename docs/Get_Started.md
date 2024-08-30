# How to get started

This document will guide you through the process of setting up the project on your local machine. It will cover the following topics:

- [Prerequisites](#prerequisites)
- [Setting up the project](#setting-up-the-project)
- [Running the project](#running-the-project)

## Prerequisites

### .env file

Create a `.env` file in the root of the project with the following content:

```bash
API_TOKEN=your_api_token_here
BUDGET_ID=your_budget_id_here
```

You can follow [This Link](https://api.ynab.com/#access-token-usage:~:text=ynab.com.-,Quick%20Start,-If%20you%27re%20the) for a guide on how to get your API token  
For the `BUDGET_ID`, you can get it from the URL of your budget page on the YNAB website. It is in between the `app.ynab.com/` and the `/budget/` in the URL. For example, if your URL is `https://app.ynab.com.com/your_budget_id/budget`, then your `BUDGET_ID` is `your_budget_id`.

## setting up the project

### Clone the repository

```bash
git clone https://github.com/Jake-Pullen/data_pipeline_for_YNAB.git
```

### Install dependencies

```bash
pip install -r requirements.txt
```

## Running the project

```bash
python3 main.py
```
