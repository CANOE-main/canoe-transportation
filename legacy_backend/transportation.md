# Legacy Transportation Backend Reference

This directory preserves the pre-v2.0 CANOE transportation backend. It is kept as validation evidence for the v2.0 refactor, not as the preferred active workflow.

## Transportation Sector Technology and Commodity Layout
The transportation sector in CANOE has various transportation modes and required infrastructure technologies, representing the movement of people and goods across different scales.

    
##Commodity
| name                    | description                                                                     | type               | units   |
|:------------------------|:--------------------------------------------------------------------------------|:-------------------|:--------|
| T\_eth                  | Ethanol for the transportation sector                                           | annual commodity   | nan     |
| T\_dsl                  | Diesel for the transportation sector                                            | annual commodity   | nan     |
| T\_rdsl                 | Renewable diesel for the transportation sector                                  | annual commodity   | nan     |
| T\_cng                  | Compressed natural gas for the transportation sector                            | annual commodity   | nan     |
| T\_jtf                  | Jet fuel for the transportation sector                                          | annual commodity   | nan     |
| T\_mdo                  | Marine diesel oil for the transportation sector                                 | annual commodity   | nan     |
| T\_lng                  | Liquefied natural gas for the transportation sector                             | annual commodity   | nan     |
| T\_elc\_ldv\_phev\_chrg | Electricity from light-duty PHEV charger                                        | annual commodity   | nan     |
| T\_elc\_ldv\_bev\_chrg  | Electricity from light-duty BEV charger                                         | annual commodity   | nan     |
| T\_elc\_hdv\_chrg       | Electricity from heavy-duty EV charger                                          | annual commodity   | nan     |
| T\_spk                  | Synthetic jet fuel for the transportation sector                                | annual commodity   | nan     |
| T\_gsl                  | Gasoline for the transportation sector                                          | annual commodity   | nan     |
| T\_gsl                  | Gasoline blendstock                                                             | annual commodity   | nan     |
| T\_eth                  | Ethanol (100% corn-based ethanol)                                               | annual commodity   | nan     |
| T\_bio                  | bioenergy for the transportation sector                                         | annual commodity   | nan     |
| T\_elc\_mdv\_chrg       | Electricity from medium-duty EV charger                                         | annual commodity   | nan     |
| T\_h2                   | Hydrogen for the transportation sector                                          | annual commodity   | nan     |
| T\_ng                   | Natural gas for the transportation sector                                       | annual commodity   | nan     |
| T\_hfo                  | Heavy fuel oil for the transportation sector                                    | annual commodity   | nan     |
| T\_lpg                  | Liquefied petroleum gas (lpg) (primarily propane) for the transportation sector | annual commodity   | nan     |
| T\_gsl\_elc\_phev35     | Gasoline and electricity for plug-in hybrid vehicles (35-mile CD range ratio)   | annual commodity   | nan     |
| T\_dsl\_rdsl04          | Diesel with 4% renewable diesel                                                 | annual commodity   | nan     |
| T\_dsl\_elc\_hdv        | Diesel and electricity for plug-in hybrid vehicles (heavy-duty class)           | annual commodity   | nan     |
| T\_cng                  | Compressed natural gas                                                          | annual commodity   | nan     |
| T\_dsl                  | Low-sulfur diesel                                                               | annual commodity   | nan     |
| T\_dsl\_elc\_mdv        | Diesel and electricity for plug-in hybrid vehicles (medium-duty class)          | annual commodity   | nan     |
| T\_hfo                  | (PJ) heavy fuel oil (transportation)                                            | annual commodity   | nan     |
| T\_rdsl                 | Renewable diesel (50% soy-based biodiesel and 50% HDRD)                         | annual commodity   | nan     |
| T\_ng                   | Natural gas for industry process                                                | annual commodity   | nan     |
| T\_mdo                  | Marine diesel oil, 0.1% sulfur content                                          | annual commodity   | nan     |
| T\_lng                  | Liquified natural gas                                                           | annual commodity   | nan     |
| T\_jtf                  | (PJ) aviation fuel (transportation)                                             | annual commodity   | nan     |
| T\_h2                   | (PJ) hydrogen (transportation)                                                  | annual commodity   | nan     |
| T\_elc                  | (PJ) electricity (transportation)                                               | annual commodity   | nan     |
| T\_spk                  | Renewable synthetic jet fuel                                                    | annual commodity   | nan     |
| T\_bio                  | (PJ) biofuel (transportation)                                                   | annual commodity   | nan     |
| T\_dsl                  | (PJ) diesel (transportation)                                                    | annual commodity   | nan     |
| T\_gsl                  | (PJ) gasoline (transportation)                                                  | annual commodity   | nan     |
| T\_bio                  | (PJ) biofuel (transportation)                                                   | annual commodity   | nan     |
| T\_elc                  | (PJ) electricity (transportation)                                               | annual commodity   | nan     |
| T\_h2\_700              | Gaseous H2 @ 700 bar                                                            | annual commodity   | nan     |
| T\_h2\_100              | Gaseous H2 @ 100 bar                                                            | annual commodity   | nan     |
| T\_h2\_10               | Gaseous H2 @ 10 bar                                                             | annual commodity   | nan     |
| T\_h2                   | Ouput hydrogen from production process                                          | annual commodity   | nan     |
| T\_gsl\_eth10           | Gasoline with at least 10% ethanol                                              | annual commodity   | nan     |
| T\_gsl\_elc\_phev50     | Gasoline and electricity for plug-in hybrid vehicles (50-mile CD range ratio)   | annual commodity   | nan     |
| T\_dsl                  | (PJ) diesel (transportation)                                                    | annual commodity   | nan     |
| T\_h2\_hdv              | Gaseous H2 @ 700 bar for use in heavy-duty vehicles                             | annual commodity   | nan     |
| T\_h2\_ldv              | Gaseous H2 @ 700 bar for use in light-duty vehicles                             | annual commodity   | nan     |
| T\_hfo                  | Heavy fuel oil, 0.1% sulfur content                                             | annual commodity   | nan     |
| T\_jtf                  | (PJ) aviation fuel (transportation)                                             | annual commodity   | nan     |
| T\_h2\_mdv              | Gaseous H2 @ 700 bar for use in medium-duty vehicles                            | annual commodity   | nan     |
| T\_hfo                  | (PJ) heavy fuel oil (transportation)                                            | annual commodity   | nan     |
| T\_gsl                  | (PJ) gasoline (transportation)                                                  | annual commodity   | nan     |
| T\_jtf\_spkX            | Conv. jet fuel, after potential blending with renewable jet fuel                | annual commodity   | nan     |
| T\_jtf                  | Petroleum conventional jet fuel                                                 | annual commodity   | nan     |
| T\_jtf\_spk50           | Conventional jet fuel with 50% HEFA SPK                                         | annual commodity   | nan     |
| T\_D\_pj\_off           | Demand for PJ by off-road vehicles                                              | demand commodity   | PJ      |
| T\_D\_pkm\_hdv\_aj      | Demand for total passenger-kilometers by jet aircrafts                          | demand commodity   | bpkm    |
| T\_D\_tkm\_hdv\_aj      | Demand for total tonne-kilometers by jet aircrafts                              | demand commodity   | btkm    |
| T\_D\_pkm\_hdv\_bs      | Demand for total passenger-kilometers driven by school and employee buses       | demand commodity   | bpkm    |
| T\_D\_pkm\_hdv\_bt      | Demand for total passenger-kilometers driven by urban transit buses             | demand commodity   | bpkm    |
| T\_D\_pkm\_hdv\_r       | Demand for total passenger-kilometers by inter-city and urban transit rails     | demand commodity   | bpkm    |
| T\_D\_pkm\_ldv\_c       | Demand for total passenger-kilometers driven by cars                            | demand commodity   | bpkm    |
| T\_D\_pkm\_ldv\_m       | Demand for total passenger-kilometers driven by motorcycles                     | demand commodity   | bpkm    |
| T\_D\_pkm\_ldv\_t       | Demand for total passenger-kilometers driven by light-duty passenger trucks     | demand commodity   | bpkm    |
| T\_D\_tkm\_hdv\_r       | Demand for total tonne-kilometers by freight rails                              | demand commodity   | btkm    |
| T\_D\_tkm\_hdv\_t       | Demand for total tonne-kilometers driven by heavy-duty trucks                   | demand commodity   | btkm    |
| T\_D\_tkm\_hdv\_wt      | Demand for total tonne-kilometers by marine vessels                             | demand commodity   | btkm    |
| T\_D\_tkm\_ldv\_t       | Demand for total tonne-kilometers driven by light-duty freight trucks           | demand commodity   | btkm    |
| T\_D\_tkm\_mdv\_t       | Demand for total tonne-kilometers driven by medium-duty trucks                  | demand commodity   | btkm    |
| T\_D\_pkm\_hdv\_bic     | Demand for total passenger-kilometers driven by inter-city buses                | demand commodity   | bpkm    |
| T\_D\_trp               | (PJ) transportation energy demand                                               | demand commodity   | PJ      |
| T\_D\_trp               | (PJ) transportation energy demand                                               | demand commodity   | PJ      |
| T\_elc                  | Electricity used by industry technologies                                       | physical commodity | nan     |
| T\_elc                  | Electricity for the transportation sector                                       | physical commodity | nan     |
| T\_elc\_dc              | Electrolysis needs DC electricity                                               | physical commodity | nan     |
| T\_ethos                | Non-physical technology used as a starting point for transportation pathways    | source commodity   | nan     |


### Passenger Demand Projection

![Passenger Demand Projection](Passenger_demand.png)


### Freight Demand Projection

![Freight Demand Projection](Freight_demand.png)

## Technology



| tech       | description                                                                                              |   unlim_cap |   annual |   reserve |   curtail |   flex |
|:-----------|:---------------------------------------------------------------------------------------------------------|------------:|---------:|----------:|----------:|-------:|]
| F\_T\_BIO  | bioenergy distribution from fuel sector to transportation sector                                         |           1 |        1 |         0 |         0 |      0 |
| E\_T\_ELC  | Electricity distribution to transportation sector                                                        |           1 |        0 |         0 |         0 |      0 |
| F\_T\_H2   | Hydrogen distribution from fuel sector to transportation sector                                          |           1 |        1 |         0 |         0 |      0 |
| F\_T\_NG   | Natural gas distribution from fuel sector to transportation sector                                       |           1 |        1 |         0 |         0 |      0 |
| F\_T\_HFO  | Heavy fuel oil distribution from fuel sector to transportation sector                                    |           1 |        1 |         0 |         0 |      0 |
| F\_T\_LPG  | Liquefied petroleum gas (lpg) (primarily propane) distribution from fuel sector to transportation sector |           1 |        1 |         0 |         0 |      0 |
| F\_T\_GSL  | Gasoline distribution from fuel sector to transportation sector                                          |           1 |        1 |         0 |         0 |      0 |
| F\_T\_ETH  | Ethanol distribution from fuel sector to transportation sector                                           |           1 |        1 |         0 |         0 |      0 |
| F\_T\_DSL  | Diesel distribution from fuel sector to transportation sector                                            |           1 |        1 |         0 |         0 |      0 |
| F\_T\_RDSL | Renewable diesel distribution from fuel sector to transportation sector                                  |           1 |        1 |         0 |         0 |      0 |
| F\_T\_CNG  | Compressed natural gas distribution from fuel sector to transportation sector                            |           1 |        1 |         0 |         0 |      0 |
| F\_T\_JTF  | Jet fuel distribution from fuel sector to transportation sector                                          |           1 |        1 |         0 |         0 |      0 |
| F\_T\_SPK  | Synthetic jet fuel distribution from fuel sector to transportation sector                                |           1 |        1 |         0 |         0 |      0 |
| F\_T\_MDO  | Marine diesel oil distribution from fuel sector to transportation sector                                 |           1 |        1 |         0 |         0 |      0 |
| F\_T\_LNG  | Liquefied natural gas distribution from fuel sector to transportation sector                             |           1 |        1 |         0 |         0 |      0 |
### Blending

| tech                      | description   |   unlim_cap |   annual |   reserve |   curtail |   flex |
|:--------------------------|:--------------|------------:|---------:|----------:|----------:|-------:|
| T\_BLND\_DSL\_ELC\_HDV    |               |           1 |        1 |         0 |         0 |      0 |
| T\_BLND\_DSL\_ELC\_MDV    |               |           1 |        1 |         0 |         0 |      0 |
| T\_BLND\_ETH\_GSL         |               |           1 |        1 |         0 |         0 |      0 |
| T\_BLND\_GSL\_ELC\_PHEV35 |               |           1 |        1 |         0 |         0 |      0 |
| T\_BLND\_GSL\_ELC\_PHEV50 |               |           1 |        1 |         0 |         0 |      0 |
| T\_BLND\_JTF              |               |           1 |        1 |         0 |         0 |      0 |
| T\_BLND\_RDSL\_DSL        |               |           1 |        1 |         0 |         0 |      0 |
| T\_BLND\_SPK              |               |           1 |        1 |         0 |         0 |      0 |
### Fuel supply

| tech                  | description   |   unlim_cap |   annual |   reserve |   curtail |   flex |
|:----------------------|:--------------|------------:|---------:|----------:|----------:|-------:|
| T\_ELC\_AC\_DC        |               |           0 |        0 |         0 |         0 |      0 |
| T\_H2\_COMP\_10\_100  |               |           0 |        1 |         0 |         0 |      0 |
| T\_H2\_COMP\_100\_700 |               |           0 |        1 |         0 |         0 |      0 |
| T\_H2\_distribution   |               |           1 |        1 |         0 |         0 |      0 |
| T\_I\_H2\_ELC\_ALK    |               |           0 |        0 |         0 |         0 |      0 |
| T\_I\_H2\_ELC\_PEM    |               |           0 |        0 |         0 |         0 |      0 |
| T\_I\_H2\_SMR         |               |           0 |        1 |         0 |         0 |      0 |
| T\_I\_H2\_SMR\_CCS    |               |           0 |        1 |         0 |         0 |      0 |
### H2 refuelling

| tech               | description   |   unlim_cap |   annual |   reserve |   curtail |   flex |
|:-------------------|:--------------|------------:|---------:|----------:|----------:|-------:|
| T\_H2\_HDV\_REFUEL |               |           0 |        1 |         0 |         0 |      0 |
| T\_H2\_LDV\_REFUEL |               |           0 |        1 |         0 |         0 |      0 |
| T\_H2\_MDV\_REFUEL |               |           0 |        1 |         0 |         0 |      0 |
### Air freight

| tech                 | description   |   unlim_cap |   annual |   reserve |   curtail |   flex |
|:---------------------|:--------------|------------:|---------:|----------:|----------:|-------:|
| T\_HDV\_AJF\_JFL\_EX |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_AJF\_JFL\_N  |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_AJF\_SPK\_N  |               |           0 |        1 |         0 |         0 |      0 |
### Air travel

| tech                 | description   |   unlim_cap |   annual |   reserve |   curtail |   flex |
|:---------------------|:--------------|------------:|---------:|----------:|----------:|-------:|
| T\_HDV\_AJP\_JFL\_EX |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_AJP\_JFL\_N  |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_AJP\_SPK\_N  |               |           0 |        1 |         0 |         0 |      0 |
### Bus

| tech                     | description   |   unlim_cap |   annual |   reserve |   curtail |   flex |
|:-------------------------|:--------------|------------:|---------:|----------:|----------:|-------:|
| T\_HDV\_BIC\_BEV\_N      |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BIC\_DSL\_EX     |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BIC\_DSL\_HEV\_N |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BIC\_DSL\_N      |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BIC\_FCEV\_N     |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BIC\_GSL\_EX     |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BIC\_GSL\_N      |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BS\_BEV\_N       |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BS\_CNG\_EX      |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BS\_CNG\_N       |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BS\_DSL\_EX      |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BS\_DSL\_HEV\_N  |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BS\_DSL\_N       |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BS\_DSL\_PHEV\_N |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BS\_FCEV\_N      |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BS\_FCHEV\_N     |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BS\_GSL\_EX      |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BS\_GSL\_N       |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BT\_BEV\_EX      |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BT\_BEV\_N       |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BT\_CNG\_EX      |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BT\_CNG\_N       |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BT\_DSL\_EX      |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BT\_DSL\_HEV\_N  |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BT\_DSL\_N       |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BT\_DSL\_PHEV\_N |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BT\_FCEV\_N      |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BT\_FCHEV\_N     |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BT\_GSL\_EX      |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_BT\_GSL\_N       |               |           0 |        1 |         0 |         0 |      0 |
### Charging

| tech               | description   |   unlim_cap |   annual |   reserve |   curtail |   flex |
|:-------------------|:--------------|------------:|---------:|----------:|----------:|-------:|
| T\_HDV\_CHRG       |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_BEV\_CHRG  |               |           0 |        0 |         0 |         0 |      0 |
| T\_LDV\_PHEV\_CHRG |               |           0 |        1 |         0 |         0 |      0 |
| T\_MDV\_CHRG       |               |           0 |        1 |         0 |         0 |      0 |
### Rail freight

| tech                | description   |   unlim_cap |   annual |   reserve |   curtail |   flex |
|:--------------------|:--------------|------------:|---------:|----------:|----------:|-------:|
| T\_HDV\_RF\_DSL\_EX |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_RF\_DSL\_N  |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_RF\_H2\_N   |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_RF\_LNG\_N  |               |           0 |        1 |         0 |         0 |      0 |
### Rail travel

| tech                  | description   |   unlim_cap |   annual |   reserve |   curtail |   flex |
|:----------------------|:--------------|------------:|---------:|----------:|----------:|-------:|
| T\_HDV\_RICP\_DSL\_EX |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_RICP\_DSL\_N  |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_RICP\_H2\_N   |               |           0 |        1 |         0 |         0 |      0 |
### Heavy trucks

| tech                    | description   |   unlim_cap |   annual |   reserve |   curtail |   flex |
|:------------------------|:--------------|------------:|---------:|----------:|----------:|-------:|
| T\_HDV\_T\_BEV\_N       |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_T\_DSL\_EX      |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_T\_DSL\_HEV\_N  |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_T\_DSL\_N       |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_T\_DSL\_PHEV\_N |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_T\_FCEV\_N      |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_T\_FCHEV\_N     |               |           0 |        1 |         0 |         0 |      0 |
### Marine freight

| tech                 | description   |   unlim_cap |   annual |   reserve |   curtail |   flex |
|:---------------------|:--------------|------------:|---------:|----------:|----------:|-------:|
| T\_HDV\_WTF\_HFO\_EX |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_WTF\_HFO\_N  |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_WTF\_LNG\_N  |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_WTF\_MDO\_EX |               |           0 |        1 |         0 |         0 |      0 |
| T\_HDV\_WTF\_MDO\_N  |               |           0 |        1 |         0 |         0 |      0 |
### Cars

| tech                       | description   |   unlim_cap |   annual |   reserve |   curtail |   flex |
|:---------------------------|:--------------|------------:|---------:|----------:|----------:|-------:|
| T\_LDV\_C\_BEV150\_EX      |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_C\_BEV150\_N       |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_C\_BEV200\_N       |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_C\_BEV300\_N       |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_C\_BEV400\_N       |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_C\_CNG\_N          |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_C\_DSL\_EX         |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_C\_DSL\_N          |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_C\_FCEV400\_N      |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_C\_GSL\_EX         |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_C\_GSL\_HEV\_EX    |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_C\_GSL\_HEV\_N     |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_C\_GSL\_N          |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_C\_GSL\_PHEV35\_EX |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_C\_GSL\_PHEV35\_N  |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_C\_GSL\_PHEV50\_N  |               |           0 |        1 |         0 |         0 |      0 |
### Light trucks

| tech                         | description   |   unlim_cap |   annual |   reserve |   curtail |   flex |
|:-----------------------------|:--------------|------------:|---------:|----------:|----------:|-------:|
| T\_LDV\_LTF\_BEV150\_N       |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTF\_BEV200\_N       |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTF\_BEV300\_N       |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTF\_BEV400\_N       |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTF\_CNG\_N          |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTF\_DSL\_EX         |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTF\_DSL\_N          |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTF\_FCEV400\_N      |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTF\_GSL\_EX         |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTF\_GSL\_HEV\_EX    |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTF\_GSL\_HEV\_N     |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTF\_GSL\_N          |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTF\_GSL\_PHEV35\_EX |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTF\_GSL\_PHEV35\_N  |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTF\_GSL\_PHEV50\_N  |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTP\_BEV150\_N       |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTP\_BEV200\_N       |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTP\_BEV300\_EX      |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTP\_BEV300\_N       |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTP\_BEV400\_N       |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTP\_CNG\_N          |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTP\_DSL\_EX         |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTP\_DSL\_N          |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTP\_FCEV400\_N      |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTP\_GSL\_EX         |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTP\_GSL\_HEV\_EX    |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTP\_GSL\_HEV\_N     |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTP\_GSL\_N          |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTP\_GSL\_PHEV35\_EX |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTP\_GSL\_PHEV35\_N  |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_LTP\_GSL\_PHEV50\_N  |               |           0 |        1 |         0 |         0 |      0 |
### Motorcycles

| tech               | description   |   unlim_cap |   annual |   reserve |   curtail |   flex |
|:-------------------|:--------------|------------:|---------:|----------:|----------:|-------:|
| T\_LDV\_M\_BEV\_N  |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_M\_GSL\_EX |               |           0 |        1 |         0 |         0 |      0 |
| T\_LDV\_M\_GSL\_N  |               |           0 |        1 |         0 |         0 |      0 |
### Medium trucks

| tech                    | description   |   unlim_cap |   annual |   reserve |   curtail |   flex |
|:------------------------|:--------------|------------:|---------:|----------:|----------:|-------:|
| T\_MDV\_T\_BEV\_N       |               |           0 |        1 |         0 |         0 |      0 |
| T\_MDV\_T\_DSL\_EX      |               |           0 |        1 |         0 |         0 |      0 |
| T\_MDV\_T\_DSL\_HEV\_N  |               |           0 |        1 |         0 |         0 |      0 |
| T\_MDV\_T\_DSL\_N       |               |           0 |        1 |         0 |         0 |      0 |
| T\_MDV\_T\_DSL\_PHEV\_N |               |           0 |        1 |         0 |         0 |      0 |
| T\_MDV\_T\_FCEV\_N      |               |           0 |        1 |         0 |         0 |      0 |
| T\_MDV\_T\_FCHEV\_N     |               |           0 |        1 |         0 |         0 |      0 |
| T\_MDV\_T\_GSL\_EX      |               |           0 |        1 |         0 |         0 |      0 |
| T\_MDV\_T\_GSL\_N       |               |           0 |        1 |         0 |         0 |      0 |
### Offroad

| tech   | description   |   unlim_cap |   annual |   reserve |   curtail |   flex |
|:-------|:--------------|------------:|---------:|----------:|----------:|-------:|
| T\_OFF |               |           1 |        1 |         0 |         0 |      0 |
