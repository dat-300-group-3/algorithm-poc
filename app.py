from ds.rtopk import RTopK

app = RTopK()
top_items = app.get_top_k()
app.print_count_top_k(top_items)
                