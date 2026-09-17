# Monitor de Resultados V2

Aplicação Flask para coletar resultados públicos da roleta JONBET, armazenar dados no Firestore e acompanhar estimativas estatísticas de cor e número.

## Recursos

- coleta sem duplicação;
- início das estimativas após 1.000 resultados únicos;
- conferência automática da previsão anterior;
- porcentagens de acertos de cor e número calculadas separadamente;
- painel responsivo para celular;
- estado e diagnóstico de erros;
- implantação por Docker no Google Cloud Run.

## Endpoints

- `/` — painel;
- `/api/health` — saúde do serviço;
- `/api/rounds` — consulta direta à fonte;
- `/api/collect` — executa uma coleta;
- `/api/stats` — estatísticas e previsão atual;
- `/api/state` — estado da última coleta;
- `/api/verify` — verificação do sistema.

## Variáveis opcionais

- `MIN_HISTORY` (padrão: `1000`);
- `HISTORY_LIMIT` (padrão: `5000`);
- `JONBET_URL` (a URL pública usada pela aplicação já é o padrão).

O Cloud Run deve usar uma conta de serviço com acesso ao Firestore. Nenhuma chave ou credencial deve ser armazenada neste repositório.

> As estimativas são experimentais e estatísticas. Resultados de jogos aleatórios não podem ser previstos com garantia.
