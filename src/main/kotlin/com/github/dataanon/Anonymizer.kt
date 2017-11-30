package com.github.dataanon

import com.github.dataanon.dsl.Whitelist
import com.github.dataanon.model.DbConfig
import com.github.dataanon.model.Field
import com.github.dataanon.model.Record
import com.github.dataanon.strategy.AnonymizationStrategy
import com.github.dataanon.strategy.number.FixedDouble
import com.github.dataanon.strategy.string.FixedString

fun main(args: Array<String>) {

    val source = DbConfig("jdbc:h2:tcp://localhost/~/movies_source", "sa", "")
    val dest = DbConfig("jdbc:h2:tcp://localhost/~/movies_dest", "sa", "")

    Whitelist(source,dest)
            .table("MOVIES") {
                where("GENRE = 'Drama'")
                limit(1_00_000)
                whitelist("MOVIE_ID","RELEASE_DATE")
                anonymize("TITLE").using(object: AnonymizationStrategy<String>{
                    override fun anonymize(field: Field<String>, record: Record): String = "MY MOVIE ${record.rowNum}"
                })
                anonymize("GENRE").using(FixedString("Action"))
            }
            .table("RATINGS") {
                whitelist("MOVIE_ID","USER_ID","CREATED_AT")
                anonymize("RATING").using(FixedDouble(4.3))
            }
            .execute()
}


