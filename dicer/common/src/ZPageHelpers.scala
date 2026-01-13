package com.databricks.dicer.common

import scalatags.Text.TypedTag
import scalatags.Text.all._

/** A set of utilities to render Zpages for Dicer (and also used by Softstore). */
object ZPageHelpers {

  /** Element name for collapse buttons. */
  private val COLLAPSE_BUTTON_NAME: String = "db-caching-collapse"

  /** `<head/>` element for Caching ZPages containing style rules. */
  val HEAD: TypedTag[String] = head {
    tag("style") {
      """
        |table, th, td {
        |  border: 1px solid black;
        |  border-collapse: collapse;
        |  text-align: left;
        |  font-weight: normal;
        |  padding: 4px
        |}
        |.collapsible {
        |  border: none;
        |  text-align: left;
        |  padding: 1px;
        |  font-size: 14px;
        |}
        |.content {
        |  display: none;
        |}
        |""".stripMargin
    }
  }

  /** Defines a button that, when clicked, will collapse/expand the next sibling element. */
  def createCollapseButton(label: String): TypedTag[String] = button(
    `type` := "button",
    style := "color:red; background-color:transparent",
    name := COLLAPSE_BUTTON_NAME,
    `class` := "collapsible",
    strong("+"),
    strong(style := "color:black", label)
  )

  /** Script defining collapse behavior for the page. */
  val COLLAPSE_SCRIPT: TypedTag[String] = script(raw(s"""
      |var coll = document.getElementsByName('$COLLAPSE_BUTTON_NAME');
      |var i;
      |for (i = 0; i < coll.length; i++) {
      |  if (coll[i].handled) { continue; }
      |
      |  // Save the original innerHTML
      |  coll[i].setAttribute('innerHTML-original', coll[i].innerHTML);
      |
      |  coll[i].handled = true;
      |  coll[i].addEventListener('click', function() {
      |    this.classList.toggle('active');
      |    var content = this.nextElementSibling;
      |    if (content.style.display === 'block') {
      |      content.style.display = 'none';
      |      this.innerHTML = this.getAttribute('innerHTML-original');
      |    } else {
      |      content.style.display = 'block';
      |      this.innerHTML = '<strong>-</strong>';
      |    }
      |  });
      |}
      |""".stripMargin))
}
