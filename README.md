# tap-cqc-org-uk

`tap-cqc-org-uk` is a Singer tap for the Care Quality Commission's API ([docs](https://anypoint.mulesoft.com/exchange/portals/care-quality-commission-5/4d36bd23-127d-4acf-8903-ba292ea615d4/cqc-syndication-1/)) which provides information on health and social care in England. Note this tap is still very much a work in progress.

The tap was built with the [Meltano Tap SDK](https://sdk.meltano.com).

## Installation

#### Pip 
This tap isn't currently published on PyPi. You can install directly with

```bash
pipx install git+https://github.com/birdiecare/tap-cqc-org-uk.git
```

#### Meltano
Similarly this tap isn't available on the meltano hub yet. You can still install it directly with 

```bash
meltano add --custom extractor tap-cqc-org-uk
```
And specifying `git+https://github.com/birdiecare/tap-cqc-org-uk.git` as the pip URL.


## Streams
- CQC_Providers: A list of health and social care provider records taken from `https://api.cqc.org.uk/public/v1/providers/` [docs](https://anypoint.mulesoft.com/exchange/portals/care-quality-commission-5/4d36bd23-127d-4acf-8903-ba292ea615d4/cqc-syndication-1/minor/1.0/console/method/%23359/).
- CQC_Locations: A list of health and social care locations records taken from `https://api.cqc.org.uk/public/v1/locations/` [docs](https://anypoint.mulesoft.com/exchange/portals/care-quality-commission-5/4d36bd23-127d-4acf-8903-ba292ea615d4/cqc-syndication-1/minor/1.0/console/method/%231190/).


## Configuration

```bash
tap-cqc-org-uk --config config_example.json
```
where `config_example.json` is 
```json
{
  "partner_code": "Your company via tap-cqc-org-uk",
  "start_date": "2021-10-27T00:00:00Z",
  "subscription_key": "XXXXXX"
}
```

- `partner_code`: a code used to identify your usage of the API to the CQC.
- `start_date`: the date from which changes will be downloaded. Note that if a record was created before this date and not updated after it then it will not be downloaded by the tap.
- `subscription_key`: Primary key got in CQC developer portal https://api-portal.service.cqc.org.uk/profile.

## Usage

You can run `tap-cqc-org-uk` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-cqc-org-uk --version
tap-cqc-org-uk --help
tap-cqc-org-uk --config CONFIG --discover > ./catalog.json
```

