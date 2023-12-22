```mermaid
erDiagram
  game_players ||--|{ games : ""
  game_players ||--|{ players : ""
  games ||--|{ kyokus : ""
  kyokus ||--|{ haipais : ""
  kyokus ||--|{ actions : ""
  kyokus ||--o| agaris : ""
  kyokus ||--o| nagares : ""

  players {
    string name PK
  }
  kyokus {
    bigint id PK
    string game_id FK
    int kyoku_num
    int honba
    int riichibou
    array(int) ten
    array(int) kazes
  }

  haipais {
    bigint kyoku_id PK,FK
    int player_index
    string haipai
  }

  actions {
    bigint kyoku_id PK,FK
    int player_index
    int seq
    string type
    string pais
  }

  agaris {
    bigint kyoku_id PK,FK
    string machihai
    int ten
    int fu
    int han
    string tehai
    array(string) yaku
    array(int) dorahai
    array(int) uradora
    int who
    int by
    array(int) score_diff
    bool owari
  }

  nagares {
    bigint kyoku_id PK,FK
    string name
    array(int) score_diff
  }

  game_players {
    string game_id PK, FK
    string player_name
    int player_index
  }
  games {
    string id PK
    bool tonpu
    bool ariari
    bool has_aka
    bool demo
    int level
    timestamp started_at
  }
```