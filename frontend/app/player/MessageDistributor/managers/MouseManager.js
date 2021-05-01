//@flow
import type StatedScreen from '../StatedScreen';
import type { MouseMove } from '../messages';
import type { Timed } from '../Timed';

import ListWalker from './ListWalker';

type MouseMoveTimed = MouseMove & Timed;

const HOVER_CLASS = "-openreplay-hover";

export default class MouseManager extends ListWalker<MouseMoveTimed> {
  #screen: StatedScreen;
	#hoverElements: Array<Element> = [];

	constructor(screen: StatedScreen): void {
		super();
		this.#screen = screen;
	}

	_updateHover(): void {
    const curHoverElements = this.#screen.cursor.getTargets();
    const diffAdd = curHoverElements.filter(elem => !this.#hoverElements.includes(elem));
    const diffRemove = this.#hoverElements.filter(elem => !curHoverElements.includes(elem));
    this.#hoverElements = curHoverElements;
    diffAdd.forEach(elem => elem.classList.add(HOVER_CLASS));
    diffRemove.forEach(elem => elem.classList.remove(HOVER_CLASS));
  }

  reset(): void {
  	this.#hoverElements = [];
  }

	move(t: number) {
		const lastMouseMove = this.moveToLast(t);
		if (!!lastMouseMove){
      this.#screen.cursor.move(lastMouseMove);
      this._updateHover();
    }
	}


}