# Windows Azure Advent Calendar 2012 2日目
今年も早いもので、あっという間に12月になりました。個人的なAzure今年の目玉は、Azure Storageのパフォーマンスの向上(Gen2)と新しくなったWindows Azure Storage 2.0です。
いろいろと新機能満載なAzureですが、ストレージ関連はクラウドの足回りとして地味に改善されているようです。

# Azure Storageのパフォーマンスの向上
2012/6/7 以降に作成されたストレージアカウントでは、下記のようにパフォーマンスターゲットが引き上げられました。Gen 2と呼ばれているようです。

* Azure Table 1Kエンティティの場合秒間のトランザクションベースだと4倍程度になっています。

1. 単一パーテーション  500 エンティティ/秒 ->   2,000 エンティティ/秒 (15Mbps)

2. 複数パーテーション 5,000 エンティティ/秒 -> 20,000 エンティティ/秒 (156Mbps)

参照：[Windows Azureのフラット ネットワーク ストレージと2012年版スケーラビリティ ターゲット](http://satonaoki.wordpress.com/2012/11/03/windows-azure%E3%81%AE%E3%83%95%E3%83%A9%E3%83%83%E3%83%88-%E3%83%8D%E3%83%83%E3%83%88%E3%83%AF%E3%83%BC%E3%82%AF-%E3%82%B9%E3%83%88%E3%83%AC%E3%83%BC%E3%82%B8%E3%81%A82012%E5%B9%B4%E7%89%88%E3%82%B9/)


# 確認しよう
早くなったということなので、Azure Storage Client 2.0を使ってGen2のパフォーマンスを確認します。ざっとソースを見た感じだと、従来のコードに比べてシンプルになって速度も期待できそうです。

前記のGen2の記事によると、エンティティが1KByteで、単一パーテーションの場合、2,000 エンティティ/秒と言うことです。このためには秒間2000オブジェクトを計測時間の間は作りづけないといけないのでCPUやGCがボトルネックになりがちです。今回は余裕を見てLargeを使うことにしました。

Largeだとメモリ7GByte、coreが8つ、ネットワーク400Mbpsというスペックなので気にしなくても良いかと思ったのですが、GCをなるべく減らすためにエンティティのデータ部分をCache（共有）します。1KByteぐらいだとあまり効果が無いかもしれませんが。


```C#
    public class EntityNk : TableEntity
    {
        const int MAX_PROPERTY = 8; 
        private static List<byte[]> dataCache;
        private static int dataSize = 1;

        static EntityNk()
        {
            Clear();
        }

        public EntityNk(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
            this.Data0 = dataCache[0];
            this.Data1 = dataCache[1];
            this.Data2 = dataCache[2];
            this.Data3 = dataCache[3];
            this.Data4 = dataCache[4];
            this.Data5 = dataCache[5];
            this.Data6 = dataCache[6];
            this.Data7 = dataCache[7];
        }

        public EntityNk() { }

        public byte[] Data0 { get; set; }
        public byte[] Data1 { get; set; }
        public byte[] Data2 { get; set; }
        public byte[] Data3 { get; set; }
        public byte[] Data4 { get; set; }
        public byte[] Data5 { get; set; }
        public byte[] Data6 { get; set; }
        public byte[] Data7 { get; set; }

        public static int DataSize
        {
            set
            {
                if (value != dataSize)
                {
                    Clear();

                    dataSize = value;
                    var x = dataSize / MAX_PROPERTY;
                    var y = dataSize % MAX_PROPERTY;

                    for (var i = 0; i < dataCache.Count(); i++)
                    {
                        dataCache[i] = GetRandomByte(x);
                    }

                    if (y != 0)
                        dataCache[x] = GetRandomByte(y);
                }
            }
        }

... 省略 ...

    }

```

さらに、Threadを上げる数を減らして並列性を上げるために非同期呼び出しを使います。.NET 4.5 から await/async が使えるので割合簡単に非同期コードが記述できるのですが、少し手間がかかりました。

なんと残念ながら、Windows Azure Storage 2.0になっても APM (Asynchronous Programming Model) のメソッドしか用意されておらず、 await で使えるTaskAsyncの形式がサポートされていません。仕方がないので、自分で拡張メソッドを書きますが、引数が多くて intellisense があっても混乱します。泣く泣く、コンパイルエラーで期待されているシグニチャーをみながら書きました。コードとしてはこんな感じで簡単です。

```C#

    public static class CloudTableExtensions
    {
        public static Task<TableResult> ExecuteAsync(this CloudTable cloudTable, TableOperation operation, TableRequestOptions requestOptions = null, OperationContext operationContext = null, object state = null)
        {
            return Task.Factory.FromAsync<TableOperation, TableRequestOptions, OperationContext, TableResult>(
                cloudTable.BeginExecute, cloudTable.EndExecute, operation, requestOptions, operationContext, state);
        }
    }

```

この辺りは、下記のサイトが詳しくお勧めです。


参照：[++C++; // 未確認飛行C 非同期処理](http://ufcpp.net/study/csharp/sp5_async.html#async)


このコードを動かしてみたら、「単一スレッド＋非同期の組み合わせだと、おおよそ２から３程度のコネクションしか作成されない」ことに気が付きました。場合によっては、5ぐらいまで上がることもあるようですが、どうしてこうなるのか不思議です。

今回のコードは複数スレッド（Task）をあげて、それぞれのスレッド内で非同期呼び出しを使って処理を行うようになっています。

さらに、上限に挑戦するためにEntity Group Transactionを使います。TableBatchOperation のインスタンスを作って操作を追加していってCloudTableのExecuteBatchAsync()で実行します。この辺りは以前の使い方とだいぶ違っています。
今回は時間を測っているいるだけですが、resultにはEntityのリストが帰ってきて、それぞれにtimestampとetagがセットされています。


```C#
            
            var batchOperation = new TableBatchOperation();

            foreach (var e in entityFactory(n))
            {
                batchOperation.Insert(e);
            }

            var cresult = new CommandResult { Start = DateTime.UtcNow.Ticks };
            var cbt = 0L;
            var context = GetOperationContext((t) => cbt = t);
            try
            {
                var results = await table.ExecuteBatchAsync(batchOperation, operationContext: context);
                cresult.Elapsed = cbt;
            }
            catch (Exception ex)
            {
                cresult.Elapsed = -1;
                Console.Error.WriteLine("Error DoInsert {0} {1}", n, ex.ToString());
            }
            return cresult;
 
```

# 結果
いくつかパラメータを調整して実行し、スロットリングが起きる前後を探して4回測定した。ピークe/sは、もっとも時間当たりのエンティティの挿入数が大きかった時の数字で秒間のエンティティ挿入数を表している。起動するプロセス数で負荷を調整している。
失敗が無かったケースでも6,684、 6,932 エンティティ/秒で処理できているので、Gen2で挙げられているパフォーマンスターゲットは十分達成できているようだ。

測定時間の、Table Metricsを見るとThrottlingErrorと同時に、ClientTimeoutErrorも出ているのでプロセスを3つ上げているケースではクライアントの負荷が高くなり過ぎている可能性が高い。

* 条件：エンティティサイズ 1KByte、単一パーテーション、スレッド数12、バッチサイズ100

<pre>
プロセス数	最少	中央値	平均	最大	90%点 	95%点	99%点	ピークe/s	成功数	失敗数
-----------------------------------------------------------------------------------------------------------------------
2 	97.27 	166.6 	258.0 	14,800 	359.578 	472.373 	1,106.282 	6,684 	40,000 	0 
2 	94.17 	260.5 	333.7 	5,320 	564.774 	723.272 	1,339.027 	6,932 	40,000 	0 
3 	90.13 	174.8 	734.1 	21,270 	1,621.490 	1,845.903 	3,434.256 	7,218 	59,377 	623 
3 	90.35 	341.6 	610.1 	27,490 	1,064.593 	1,380.415 	4,431.789 	8,005 	59,740 	260 
</pre>


# 最後に
このレポジトリに計測に使ったコードをいれてありますので見てみてください。
12/2のはずでしたが、だいぶ遅くなってしました。データの解析に慣れない「R」を使ったのですが、どうも慣れなくて手間取ってしまいました。最初はRで出した図なども入れたいと思ったのですが、軸や凡例の設定がうまくできずに時間切れで断念してしまいました。

